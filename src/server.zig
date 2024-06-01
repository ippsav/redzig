const std = @import("std");
const RespData = @import("resp/encoding.zig").RespData;
const Connection = std.net.Server.Connection;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const RWMutex = std.Thread.RwLock;

pub const Store = struct {
    allocator: std.mem.Allocator,
    map: std.StringHashMapUnmanaged(RespData),
    mutex: RWMutex = .{},

    pub fn init(allocator: std.mem.Allocator) Store {
        return Store{ .map = std.StringHashMapUnmanaged(RespData){}, .allocator = allocator };
    }

    pub fn deinit(self: *Store) void {
        var entry_it = self.map.iterator();

        while (entry_it.next()) |entry| {
            self.allocator.free(entry.key_ptr.*);
            entry.value_ptr.deinit(self.allocator);
        }
        self.map.deinit(self.allocator);
    }

    pub fn get(self: *Store, key: []const u8) ?RespData {
        self.mutex.lockShared();
        defer self.mutex.unlockShared();

        return self.map.get(key);
    }

    pub fn put(self: *Store, key: []const u8, value: RespData) !void {
        self.mutex.lock();
        defer self.mutex.unlock();

        const res = try self.map.getOrPut(self.allocator, key);

        if (!res.found_existing) {
            const k = try self.allocator.dupe(u8, key);
            res.key_ptr.* = k;
        } else {
            res.value_ptr.deinit(self.allocator);
        }

        try RespData.dupe(self.allocator, value, res.value_ptr);
    }

    pub fn debug(self: *Store) void {
        var entry_it = self.map.iterator();

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

    fn handleConnection(server: *Server, connection: Connection) !void {
        std.debug.print("connection accepted\n", .{});
        const reader = connection.stream.reader();
        defer connection.stream.close();

        var parser = RespParser.init(server.allocator);

        while (true) {
            var data = try parser.readStream(reader) orelse break;
            try server.handleCommand(connection, data);

            data.deinit(server.allocator);
        }
    }

    pub fn handleCommand(self: *Server, connection: Connection, data: RespData) !void {
        const cmd_str = data.array[0].bulk_string;
        const cmd = command.getCommandEnum(cmd_str).?;

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
            try std.fmt.format(connection.stream.writer(), "_\r\n", .{});
            return;
        };

        std.debug.assert(value == .bulk_string);

        try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ value.bulk_string.len, value.bulk_string });
    }

    fn handleSetCommand(self: *Server, connection: Connection, parsed_data: RespData) !void {
        const key = parsed_data.array[1].bulk_string;
        const value = parsed_data.array[2];

        try self.store.put(key, value);
        self.store.debug();

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
