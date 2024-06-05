const std = @import("std");

pub const Role = enum {
    master,
    slave,
};

pub const NodeConfig = union(Role) {
    master: void,
    slave: SlaveConfig,
};

pub const SlaveConfig = struct { host: []const u8, port: u16 };

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 6379,

    role: Role = undefined,
    node_config: NodeConfig = undefined,

    pub fn format(self: Config, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("host={s}, port={d}, role={s}, node_config={s}", .{ self.host, self.port, @tagName(self.role), @tagName(self.node_config) });
        if (self.role == .slave) {
            try writer.print("\ns_host={s}, s_port={d}", .{ self.node_config.slave.host, self.node_config.slave.port });
        }
    }
};