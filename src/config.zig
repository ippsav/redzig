const std = @import("std");

pub const Role = enum {
    none,
    master,
    slave,
};

pub const NodeConfig = union(Role) {
    none: void,
    master: MasterConfig,
    slave: SlaveConfig,
};

pub const MasterConfig = struct {
    master_replid: []const u8,
    master_repl_offset: u64 = 0,
};

pub const SlaveConfig = struct { address: std.net.Address };

pub const Config = struct {
    host: []const u8 = "127.0.0.1",
    port: u16 = 6379,

    role: Role = .none,
    node_config: NodeConfig = .{ .none = {} },

    pub fn configureSlaveNode(cfg: *Config, address: std.net.Address) void {
        cfg.role = .slave;
        cfg.node_config = .{ .slave = .{ .address = address } };
    }

    pub fn configureMasterNode(cfg: *Config) void {
        cfg.role = .master;
        cfg.node_config = .{ .master = .{
            .master_replid = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb",
            .master_repl_offset = 0,
        } };
    }

    pub fn format(cfg: Config, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        try writer.print("host={s}, port={d}, role={s}, node_config={s}", .{ cfg.host, cfg.port, @tagName(cfg.role), @tagName(cfg.node_config) });
        if (cfg.role == .slave) {
            try writer.print("\ns_address={}", .{cfg.node_config.slave.address});
        }
    }
};
