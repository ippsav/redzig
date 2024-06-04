const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const Connection = std.net.Server.Connection;
const Server = @import("server.zig").Server;
const Store = @import("server.zig").Store;

const Args = union(enum) {
    port: u16,
};

fn parseArgs(allocator: std.mem.Allocator, args: [][]const u8) ![]Args {
    var i: usize = 1;
    var parsed_args = std.ArrayList(Args).init(allocator);

    while (i < args.len) : (i += 1) {
        const arg = args[i];

        if (std.mem.eql(u8, arg, "--port")) {
            if (i + 1 >= args.len) return error.MissingPort;
            const port = try std.fmt.parseInt(u16, args[i + 1], 10);

            try parsed_args.append(.{ .port = port });
        }
    }
    return try parsed_args.toOwnedSlice();
}

pub fn main() !void {
    const allocator = std.heap.c_allocator;

    var port: u16 = 6379;

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const parsed_args = try parseArgs(allocator, args);

    for (parsed_args) |arg| {
        switch (arg) {
            .port => |p| {
                port = p;
            },
        }
    }

    var store = Store.init(allocator);
    defer {
        store.deinit();
    }

    std.debug.print("serving on port: {d}\n", .{port});
    var server = try Server.init(allocator, &store, "127.0.0.1", port);

    try server.startExpirationWorker();
    try server.start();
}
