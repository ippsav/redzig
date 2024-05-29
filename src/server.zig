const std = @import("std");
const RespData = @import("resp/encoding.zig").RespData;
const Connection = std.net.Server.Connection;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const RWMutex = std.Thread.RwLock;

pub const Server = struct {
    allocator: std.mem.Allocator,
    store: std.StringHashMap(RespData),
    address: std.net.Address,

    pub fn init(allocator: std.mem.Allocator, host: []const u8, port: u16) !Server {
        const address = try std.net.Address.resolveIp(host, port);

        return .{ .allocator = allocator, .store = std.StringHashMap(RespData).init(allocator), .address = address };
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
            else => unreachable,
        }
    }

    fn handleSetCommand(_: *Server, connection: Connection, parsed_data: RespData) !void {
        // if (parsed_data.array.len == 1) {
        //     _ = try connection.stream.write("+PONG\r\n");
        //     return;
        // }
        // const str = parsed_data.array[1].bulk_string;
        // std.debug.print("ECHO: {s}\n", .{str});
        // try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
        _ = connection;
        std.debug.print("{}", .{parsed_data});
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
