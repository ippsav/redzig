const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const Connection = std.net.Server.Connection;
const Server = @import("server.zig").Server;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var server = try Server.init(allocator, "127.0.0.1", 6379);

    try server.start();
}
