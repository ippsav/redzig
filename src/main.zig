const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const Connection = std.net.Server.Connection;
const Server = @import("server.zig").Server;
const Store = @import("server.zig").Store;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();
    var store = Store.init(allocator);

    defer {
        store.deinit();
        _ = gpa.deinit();
    }

    var server = try Server.init(allocator, &store, "127.0.0.1", 6379);

    try server.start();
}
