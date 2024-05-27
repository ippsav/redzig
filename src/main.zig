const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const Connection = std.net.Server.Connection;

pub fn handleConnection(allocator: std.mem.Allocator, connection: Connection) !void {
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

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const address = try net.Address.resolveIp("127.0.0.1", 6379);

    var listener = try address.listen(.{
        .reuse_address = true,
    });
    defer listener.deinit();

    while (true) {
        const connection = try listener.accept();
        _ = try std.Thread.spawn(.{}, handleConnection, .{ allocator, connection });
    }
}
