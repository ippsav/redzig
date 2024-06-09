const std = @import("std");
const RespData = @import("../resp/encoding.zig").RespData;

pub fn echoValue(writer: std.io.AnyWriter, value: RespData) !void {
    std.debug.assert(value == .bulk_string);

    const str = value.bulk_string;
    try std.fmt.format(writer, "${d}\r\n{s}\r\n", .{ str.len, str });
}
