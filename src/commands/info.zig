const std = @import("std");
const Config = @import("../config.zig").Config;
const utils = @import("../utils.zig");

pub const InfoCommandArgs = enum { replication };

pub fn handleInfoReplication(allocator: std.mem.Allocator, writer: std.io.AnyWriter, config: Config) !void {
    switch (config.node_config) {
        .master => |m_config| {
            const info = try std.fmt.allocPrint(allocator, "role:master\nmaster_replid:{s}\nmaster_repl_offset:{d}", .{ m_config.master_replid, m_config.master_repl_offset });
            defer allocator.free(info);
            try std.fmt.format(writer, "${d}\r\n{s}\r\n", .{ info.len, info });
        },
        .slave => {
            try std.fmt.format(writer, "$10\r\nrole:slave\r\n", .{});
        },
        else => unreachable,
    }
}
