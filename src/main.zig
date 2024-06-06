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
    replicaof: std.net.Address,
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
            var address: std.net.Address = undefined;
            if (i + 2 < args.len and !std.mem.startsWith(u8, args[i + 1], "--")) {
                const r_host = blk: {
                    if (std.mem.eql(u8, args[i + 1], "localhost")) {
                        break :blk "127.0.0.1";
                    }
                    break :blk args[i + 1];
                };
                const r_port = try std.fmt.parseInt(u16, args[i + 2], 10);

                address = try net.Address.parseIp(r_host, r_port);
            } else if (i + 1 < args.len) {
                var it = std.mem.tokenize(u8, args[i + 1], " ");
                const r_host = blk: {
                    const h = it.next() orelse return error.InvalidReplicaOf;
                    if (std.mem.eql(u8, h, "localhost")) {
                        break :blk "127.0.0.1";
                    }
                    break :blk h;
                };
                const r_port_str = it.next() orelse return error.InvalidReplicaOf;

                const r_port = try std.fmt.parseInt(u16, r_port_str, 10);

                address = try net.Address.parseIp(r_host, r_port);
            } else {
                return error.MissingReplicaof;
            }
            try parsed_args.append(.{ .replicaof = address });
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
            .replicaof => |address| {
                s_config.configureSlaveNode(address);
            },
        }
    }

    if (s_config.role == .none) {
        s_config.configureMasterNode();
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
