const std = @import("std");
const RespData = @import("./encoding.zig").RespData;
const Connection = std.net.Server.Connection;

pub const Command = enum { ping, echo, set, get };

pub fn getCommandEnum(command: []const u8) ?Command {
    const fields = std.meta.fields(Command);

    inline for (fields) |field| {
        if (std.ascii.eqlIgnoreCase(field.name, command)) {
            return @field(Command, field.name);
        }
    }
    return null;
}

fn handlePingCommand(connection: Connection, parsed_data: RespData) !void {
    if (parsed_data.array.len == 1) {
        _ = try connection.stream.write("+PONG\r\n");
        return;
    }
    const str = parsed_data.array[1].bulk_string;
    std.debug.print("ECHO: {s}\n", .{str});
    try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
}

fn handleEchoCommand(connection: Connection, parsed_data: RespData) !void {
    const str = parsed_data.array[1].bulk_string;
    std.debug.print("ECHO: {s}\n", .{str});
    try std.fmt.format(connection.stream.writer(), "${d}\r\n{s}\r\n", .{ str.len, str });
}

pub fn handleCommand(connection: Connection, parsed_data: RespData) !void {
    const command = parsed_data.array[0].bulk_string;

    const command_enum = getCommandEnum(command).?;
    std.debug.print("{?}\n", .{command_enum});

    switch (command_enum) {
        .ping => try handlePingCommand(connection, parsed_data),
        .echo => try handleEchoCommand(connection, parsed_data),
        else => unreachable,
    }
}
