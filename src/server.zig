const std = @import("std");
const RespData = @import("resp/encoding.zig").RespData;
const Connection = std.net.Server.Connection;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const utils = @import("utils.zig");
const RWMutex = std.Thread.RwLock;
const Config = @import("config.zig").Config;

pub const DurationState = struct {
    exp: i64,
    created_at: i64,
};

pub const SetOptionalParams = enum { ex, px };

pub const Store = struct {
    allocator: std.mem.Allocator,
    cache_map: std.StringHashMapUnmanaged(RespData),
    expiration_map: std.StringHashMapUnmanaged(DurationState),
    mutex: RWMutex = .{},

    pub fn init(allocator: std.mem.Allocator) Store {
        return Store{ .cache_map = std.StringHashMapUnmanaged(RespData){}, .expiration_map = std.StringHashMapUnmanaged(DurationState){}, .allocator = allocator };
    }

    pub fn deinit(self: *Store) void {
        var entry_it = self.cache_map.iterator();

        while (entry_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.cache_map.deinit(self.allocator);
        self.expiration_map.deinit(self.allocator);
    }

    pub fn get(self: *Store, key: []const u8) ?RespData {
        self.mutex.lock();
        defer self.mutex.unlock();

        const entry = self.cache_map.getEntry(key) orelse {
            return null;
        };
        const exp_state = self.expiration_map.get(key);

        if (exp_state) |state| {
            if (state.exp < std.time.milliTimestamp() - state.created_at) {
                // store pointer to string to clear it
                const str_to_clear = entry.key_ptr.*;

                _ = self.expiration_map.remove(key);
                entry.value_ptr.deinit(self.allocator);
                _ = self.cache_map.remove(key);

                self.allocator.free(str_to_clear);

                return null;
            }
        }

        return entry.value_ptr.*;
    }

    pub fn clearBatchOfExpiredEntries(self: *Store, comptime number_of_entries: usize) void {
        self.mutex.lock();
        defer self.mutex.unlock();

        var entry_it = self.expiration_map.iterator();

        var i: usize = 0;
        while (entry_it.next()) |exp_entry| {
            if (i >= number_of_entries) break;
            if (exp_entry.value_ptr.exp < std.time.milliTimestamp() - exp_entry.value_ptr.created_at) {
                i += 1;
                var entry = self.cache_map.fetchRemove(exp_entry.key_ptr.*).?;

                // store pointer to string to clear it
                std.debug.print("removing\n\tkey: {s}\n\t{}\n", .{ entry.key, entry.value });
                const str_to_clear = entry.key;

                _ = self.expiration_map.remove(exp_entry.key_ptr.*);
                entry.value.deinit(self.allocator);
                self.allocator.free(str_to_clear);
            }
        }
    }

    pub fn put(self: *Store, key: []const u8, value: RespData, expiration_ms: ?i64) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const res = try self.cache_map.getOrPut(self.allocator, key);

        if (!res.found_existing) {
            const k = try self.allocator.dupe(u8, key);
            res.key_ptr.* = k;
            if (expiration_ms) |duration| {
                const state = DurationState{ .exp = duration, .created_at = std.time.milliTimestamp() };
                try self.expiration_map.put(self.allocator, k, state);
            }
        } else {
            res.value_ptr.deinit(self.allocator);
        }

        try RespData.dupe(self.allocator, value, res.value_ptr);
    }

    pub fn debug(self: *Store) void {
        var entry_it = self.cache_map.iterator();

        while (entry_it.next()) |entry| {
            std.debug.print("{s}: {s}\n", .{ entry.key_ptr.*, entry.value_ptr.*.bulk_string });
        }
    }
};

pub const Server = struct {
    allocator: std.mem.Allocator,
    store: *Store,
    address: std.net.Address,
    config: Config,

    pub fn init(allocator: std.mem.Allocator, store: *Store, config: Config) !Server {
        const address = try std.net.Address.resolveIp(config.host, config.port);

        return .{ .allocator = allocator, .store = store, .address = address, .config = config };
    }

    pub fn start(self: *Server) !void {
        if (self.config.role == .slave) {
            var parser = RespParser.init(self.allocator);

            const stream = try std.net.tcpConnectToAddress(self.config.node_config.slave.address);
            try sendPingMessage(stream);
            var parsed_data = try parser.readStream(stream.reader());
            if (parsed_data != null) {
                var data = parsed_data.?;
                defer data.deinit(self.allocator);
                std.debug.print("got data: {s}\n", .{data});
            }
            try sendReplConfListeningPortMessage(self.config.port, stream);
            parsed_data = try parser.readStream(stream.reader()) orelse return error.InvalidReplicationConfig;
            if (parsed_data != null) {
                var data = parsed_data.?;
                defer data.deinit(self.allocator);
                std.debug.print("got data: {s}\n", .{data});
            }

            try sendReplConfListeningCapaMessage(stream);
            parsed_data = try parser.readStream(stream.reader()) orelse return error.InvalidReplicationConfig;
            if (parsed_data != null) {
                var data = parsed_data.?;
                defer data.deinit(self.allocator);
                std.debug.print("got data: {s}\n", .{data});
            }

            try sendPsyncCommand(stream);
            parsed_data = try parser.readStream(stream.reader()) orelse return error.InvalidPsyncResponse;
            if (parsed_data != null) {
                var data = parsed_data.?;
                defer data.deinit(self.allocator);
                std.debug.print("got data: {s}\n", .{data});
            }

            var rdb_data = std.ArrayList(u8).init(self.allocator);
            defer rdb_data.deinit();
            var buffer: [1024]u8 = undefined;
            while (true) {
                const bytes_read = try stream.reader().read(&buffer);
                if (bytes_read == 0) break;
                try rdb_data.appendSlice(buffer[0..bytes_read]);
                if (bytes_read < buffer.len) break;
            }

            std.debug.print("got rdb data: {s}\n", .{rdb_data.items});
        }

        var listener = try self.address.listen(.{
            .reuse_address = true,
        });
        defer listener.deinit();

        while (true) {
            const connection = try listener.accept();
            _ = try std.Thread.spawn(.{}, handleConnection, .{ self, connection });
        }
    }

    fn sendPingMessage(stream: std.net.Stream) !void {
        try std.fmt.format(stream.writer(), "*1\r\n$4\r\nPING\r\n", .{});
    }

    fn sendPsyncCommand(stream: std.net.Stream) !void {
        try std.fmt.format(stream.writer(), "*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n", .{});
    }

    fn sendReplConfListeningPortMessage(port: u16, stream: std.net.Stream) !void {
        try std.fmt.format(stream.writer(), "*3\r\n$8\r\nREPLCONF\r\n$14\r\nlistening-port\r\n$4\r\n{d}\r\n", .{port});
    }

    fn sendReplConfListeningCapaMessage(stream: std.net.Stream) !void {
        try std.fmt.format(stream.writer(), "*3\r\n$8\r\nREPLCONF\r\n$4\r\ncapa\r\n$6\r\npsync2\r\n", .{});
    }

    pub fn startExpirationWorker(self: *Server) !void {
        _ = try std.Thread.spawn(.{}, handleExpiration, .{self});
    }

    fn handleExpiration(self: *Server) !void {
        while (true) {
            std.time.sleep(std.time.ns_per_ms * 100);
            self.store.clearBatchOfExpiredEntries(20);
        }
    }

    fn handleConnection(server: *Server, connection: Connection) !void {
        std.debug.print("connection accepted\n", .{});
        const reader = connection.stream.reader();
        defer connection.stream.close();

        var arena = std.heap.ArenaAllocator.init(server.allocator);
        defer arena.deinit();
        const child_allocator = arena.allocator();

        var parser = RespParser.init(child_allocator);

        while (true) {
            defer _ = arena.reset(.retain_capacity);
            const data = try parser.readStream(reader) orelse break;
            try server.handleCommand(connection.stream, data);
        }
    }

    pub fn handleCommand(self: *Server, stream: std.net.Stream, data: RespData) !void {
        const cmd_str = data.array[0].bulk_string;
        const cmd = utils.getEnumIgnoreCase(command.Command, cmd_str).?;

        switch (cmd) {
            .ping => try self.handlePingCommand(stream, data),
            .echo => try self.handleEchoCommand(stream, data),
            .set => try self.handleSetCommand(stream, data),
            .get => try self.handleGetCommand(stream, data),
            .info => try self.handleInfoCommand(stream, data),
            .replconf => try self.handleReplconfCommand(stream, data),
            .psync => try self.handlePsyncCommand(stream, data),
        }
    }

    fn handlePsyncCommand(server: *Server, stream: std.net.Stream, _: RespData) !void {
        try std.fmt.format(stream.writer(), "+FULLRESYNC {s} 0\r\n", .{server.config.node_config.master.master_replid});

        const rdb_b64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";

        const base64_decoder = std.base64.Base64Decoder.init(std.base64.standard.alphabet_chars, std.base64.standard.pad_char);

        const len = try base64_decoder.calcSizeForSlice(rdb_b64);
        const decoded_data = try server.allocator.alloc(u8, len);
        defer server.allocator.free(decoded_data);

        try base64_decoder.decode(decoded_data, rdb_b64);

        std.debug.print("sending rdb len: {d}\n", .{len});

        try std.fmt.format(stream.writer(), "${d}\r\n{s}", .{ len, decoded_data });
    }

    fn handleReplconfCommand(_: *Server, stream: std.net.Stream, _: RespData) !void {
        _ = try stream.writer().write("+OK\r\n");
    }

    fn handleInfoCommand(self: *Server, stream: std.net.Stream, parsed_data: RespData) !void {
        const InfoCommandArgs = enum { replication };

        const arg = utils.getEnumIgnoreCase(InfoCommandArgs, parsed_data.array[1].bulk_string) orelse return error.InvalidCommand;

        switch (arg) {
            .replication => {
                switch (self.config.node_config) {
                    .master => |m_config| {
                        const info = try std.fmt.allocPrint(self.allocator, "role:master\nmaster_replid:{s}\nmaster_repl_offset:{d}", .{ m_config.master_replid, m_config.master_repl_offset });
                        defer self.allocator.free(info);
                        try std.fmt.format(stream.writer(), "${d}\r\n{s}\r\n", .{ info.len, info });
                    },
                    .slave => {
                        try std.fmt.format(stream.writer(), "$10\r\nrole:slave\r\n", .{});
                    },
                    else => unreachable,
                }
            },
        }
    }

    fn handleGetCommand(self: *Server, stream: std.net.Stream, parsed_data: RespData) !void {
        const key = parsed_data.array[1].bulk_string;

        const value = self.store.get(key) orelse {
            try std.fmt.format(stream.writer(), "$-1\r\n", .{});
            return;
        };

        std.debug.assert(value == .bulk_string);

        try std.fmt.format(stream.writer(), "${d}\r\n{s}\r\n", .{ value.bulk_string.len, value.bulk_string });
    }

    fn handleSetCommand(self: *Server, stream: std.net.Stream, parsed_data: RespData) !void {
        const ParsingSetCommandStep = enum { key, value, expiry };

        var step: ParsingSetCommandStep = .key;

        var key: []const u8 = undefined;
        var value: RespData = undefined;

        var opt_param_enum: ?SetOptionalParams = null;

        var expiration_ms: ?i64 = null;

        if (parsed_data.array.len < 3) return error.InvalidCommand;

        var i: usize = 1;
        while (i < parsed_data.array.len) : (i += 1) {
            const arg = parsed_data.array[i];

            if (i >= 3 and i % 2 != 0) {
                opt_param_enum = utils.getEnumIgnoreCase(SetOptionalParams, arg.bulk_string) orelse return error.InvalidOptionalParam;
                switch (opt_param_enum.?) {
                    .ex, .px => {
                        std.debug.assert(expiration_ms == null);
                        step = .expiry;
                    },
                }
            }
            switch (step) {
                .key => {
                    key = arg.bulk_string;
                    step = .value;
                },
                .value => {
                    value = arg;
                    if (parsed_data.array.len == 3) break;
                },
                .expiry => {
                    i += 1;
                    if (i >= parsed_data.array.len) return error.MissingExpiration;
                    const expiration_arg = parsed_data.array[i];
                    expiration_ms = try std.fmt.parseInt(i64, expiration_arg.bulk_string, 10);
                    if (opt_param_enum == .ex) {
                        expiration_ms = expiration_ms.? * 1000;
                    }
                    step = .value; // Reset to value for further parsing if needed
                },
            }
        }

        try self.store.put(key, value, expiration_ms);

        try std.fmt.format(stream.writer(), "$2\r\nOK\r\n", .{});
    }

    fn handlePingCommand(_: *Server, stream: std.net.Stream, parsed_data: RespData) !void {
        if (parsed_data.array.len == 1) {
            _ = try stream.write("+PONG\r\n");
            return;
        }

        for (parsed_data.array) |arg| {
            std.debug.print("{arg}\n", .{arg});
        }

        const str = parsed_data.array[1].bulk_string;
        try std.fmt.format(stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
    }

    fn handleEchoCommand(_: *Server, stream: std.net.Stream, parsed_data: RespData) !void {
        const str = parsed_data.array[1].bulk_string;
        try std.fmt.format(stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
    }
};
