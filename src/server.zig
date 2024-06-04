const std = @import("std");
const RespData = @import("resp/encoding.zig").RespData;
const Connection = std.net.Server.Connection;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const utils = @import("utils.zig");
const RWMutex = std.Thread.RwLock;

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

    pub fn init(allocator: std.mem.Allocator, store: *Store, host: []const u8, port: u16) !Server {
        const address = try std.net.Address.resolveIp(host, port);

        return .{ .allocator = allocator, .store = store, .address = address };
    }

    pub fn start(self: *Server) !void {
        var listener = try self.address.listen(.{
            .reuse_address = true,
        });
        defer listener.deinit();

        while (true) {
            const connection = try listener.accept();
            _ = try std.Thread.spawn(.{}, handleConnection, .{ self, connection });
        }
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
            try server.handleCommand(connection, data);
        }
    }

    pub fn handleCommand(self: *Server, connection: Connection, data: RespData) !void {
        const cmd_str = data.array[0].bulk_string;
        const cmd = utils.getEnumIgnoreCase(command.Command, cmd_str).?;

        switch (cmd) {
            .ping => try self.handlePingCommand(connection, data),
            .echo => try self.handleEchoCommand(connection, data),
            .set => try self.handleSetCommand(connection, data),
            .get => try self.handleGetCommand(connection, data),
        }
    }

    fn handleGetCommand(self: *Server, connection: Connection, parsed_data: RespData) !void {
        const key = parsed_data.array[1].bulk_string;

        const value = self.store.get(key) orelse {
            try std.fmt.format(connection.stream.writer(), "$-1\r\n", .{});
            return;
        };

        std.debug.assert(value == .bulk_string);

        try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ value.bulk_string.len, value.bulk_string });
    }

    fn handleSetCommand(self: *Server, connection: Connection, parsed_data: RespData) !void {
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

        try std.fmt.format(connection.stream.writer(), "$2\r\nOK\r\n", .{});
    }

    fn handlePingCommand(_: *Server, connection: Connection, parsed_data: RespData) !void {
        if (parsed_data.array.len == 1) {
            _ = try connection.stream.write("+PONG\r\n");
            return;
        }

        const str = parsed_data.array[1].bulk_string;
        std.debug.print("ECHO: {s}\n", .{str});
        try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
    }

    fn handleEchoCommand(_: *Server, connection: Connection, parsed_data: RespData) !void {
        const str = parsed_data.array[1].bulk_string;
        std.debug.print("ECHO: {s}\n", .{str});
        try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
    }
};
