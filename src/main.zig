const std = @import("std");
const net = std.net;
const RespParser = @import("resp/encoding.zig").RespParser;
const command = @import("resp/command.zig");
const Connection = std.net.Server.Connection;
const Server = @import("server.zig").Server;
const Store = @import("server.zig").Store;
const config = @import("config.zig");

const Args = union(enum) {
    port: u16,
    replicaof: []const u8,
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
        } else if (std.mem.eql(u8, arg, "--replicaof")) {
            if (i + 1 >= args.len) return error.MissingReplicaOf;
            const replicaof = args[i + 1];
            try parsed_args.append(.{ .replicaof = replicaof });
        }
    }
    return try parsed_args.toOwnedSlice();
}

pub fn main() !void {
    // const allocator = std.heap.c_allocator;
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const parsed_args = try parseArgs(allocator, args);

    var s_config: config.Config = .{};

    for (parsed_args) |arg| {
        switch (arg) {
            .port => |p| {
                s_config.port = p;
            },
            .replicaof => |r| {
                var it = std.mem.tokenize(u8, r, " ");

                const r_host = it.next() orelse return error.InvalidReplicaOf;
                const r_port_str = it.next() orelse return error.InvalidReplicaOf;

                const r_port = try std.fmt.parseInt(u16, r_port_str, 10);

                s_config.role = .slave;
                s_config.node_config = .{ .slave = .{
                    .port = r_port,
                    .host = r_host,
                } };
            },
        }
    }

    var store = Store.init(allocator);
    defer {
        store.deinit();
        _ = gpa.deinit();
    }

    std.debug.print("serving on port: {d}\n", .{s_config.port});
    var server = try Server.init(allocator, &store, s_config);

    try server.startExpirationWorker();
    try server.start();
}
