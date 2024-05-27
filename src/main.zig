const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;

pub fn main() !void {
    const stdout = std.io.getStdOut().writer();

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
        std.debug.print("connection accepted\n", .{});

        const reader = connection.stream.reader();

        var parser = RespParser.init(allocator);

        while (try parser.readStream(reader)) |_| {
            _ = try connection.stream.write("+PONG\r\n");
        }

        try stdout.print("accepted new connection\n", .{});
        connection.stream.close();
    }
}
