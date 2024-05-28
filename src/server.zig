const std = @import("std");
const RespData = @import("resp/encoding.zig").RespData;
const Connection = std.net.Server.Connection;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const RWMutex = std.Thread.RwLock;

fn handleConnection(allocator: std.mem.Allocator, connection: Connection) !void {
    std.debug.print("connection accepted\n", .{});
    const reader = connection.stream.reader();
    defer connection.stream.close();

    var parser = RespParser.init(allocator);

    while (true) {
        var data = try parser.readStream(reader) orelse break;
        try command.handleCommand(connection, data);

        data.deinit(allocator);
    }
}

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
            _ = try std.Thread.spawn(.{}, handleConnection, .{ self.allocator, connection });
        }
    }
};
