const std = @import("std");
const TokenIterator = std.mem.TokenIterator;

const U8SequenceIterator = TokenIterator(u8, .sequence);

// Types
// Simple strings   +
// Simple Errors    -
// Simple Integers  :
// Bulk strings     $
// Arrays           *
// Nulls            _
// Booleans         #
// Doubles          ,
// Big numbers      (
// Bulk errors      !
// Verbatim strings =
// Maps             %
// Sets             ~
// Pushes           >

pub const RespData = union(enum) {
    string: []const u8,
    @"error": []const u8,
    integer: i64,
    bulk_string: []const u8,
    array: []RespData,
    null,
    boolean: bool,
    double: f64,
    bigint: i128,
    errors: [][]const u8,

    pub fn dupe(allocator: std.mem.Allocator, in: RespData, out: *RespData) !void {
        switch (in) {
            .bulk_string => |str| {
                std.debug.print("str: {s}, len: {d}\n", .{ str, str.len });
                out.* = .{ .bulk_string = try allocator.dupe(u8, str) };
            },
            .array => |array| {
                const new_array = try allocator.alloc(RespData, array.len);

                for (array, new_array) |val, *new| {
                    try dupe(allocator, val, new);
                }
                out.* = .{ .array = new_array };
            },
            else => unreachable,
        }
    }

    pub fn deinit(self: *RespData, allocator: std.mem.Allocator) void {
        switch (self.*) {
            .bulk_string => |str| allocator.free(str),
            .array => |array| {
                for (array) |*resp_data| {
                    resp_data.deinit(allocator);
                }
                allocator.free(array);
            },
            else => unreachable,
        }
    }

    pub fn format(self: RespData, comptime _: []const u8, _: std.fmt.FormatOptions, writer: anytype) !void {
        switch (self) {
            .string => {
                try writer.print("string: {s}", .{self.string});
            },
            .@"error" => {
                try writer.print("error: {s}", .{self.@"error"});
            },
            .integer => {
                try writer.print("integer: {d}", .{self.integer});
            },
            .bulk_string => {
                try writer.print("bulk_string: {s}", .{self.bulk_string});
            },
            .array => {
                try writer.print("array:\n", .{});
                for (self.array) |resp_data| {
                    try writer.print("  {}\n", .{resp_data});
                }
            },
            .null => {
                try writer.print("null", .{});
            },
            .boolean => {
                try writer.print("boolean: {any}", .{self.boolean});
            },
            .double => {
                try writer.print("double: {d}", .{self.double});
            },
            .bigint => {
                try writer.print("bigint: {d}", .{self.bigint});
            },
            .errors => {
                try writer.print("errors:\n", .{});
                for (self.errors) |err| {
                    try writer.print("  {s}", .{err});
                }
            },
        }
    }
};

pub const RespParser = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Self {
        return Self{
            .allocator = allocator,
        };
    }

    // connection accepted
    // reading...
    // received data: *1
    // $4
    // PING
    //
    // received data: *2
    // $4
    // ECHO
    // $4
    // word
    pub fn parseArray(self: *Self, lines: *U8SequenceIterator) error{ Overflow, InvalidCharacter, OutOfMemory }!RespData {
        const curr = lines.next().?;

        const length = try std.fmt.parseInt(usize, curr[1..2], 10);

        var resp_array = try std.ArrayList(RespData).initCapacity(self.allocator, length);
        defer resp_array.deinit();

        for (0..length) |_| {
            const resp_data = try self.parse(lines);
            try resp_array.append(resp_data);
        }

        return .{
            .array = try resp_array.toOwnedSlice(),
        };
    }

    pub fn parseBulkString(self: *Self, lines: *U8SequenceIterator) !RespData {
        const length_line = lines.next().?;
        const length = try std.fmt.parseInt(usize, length_line[1..], 10);

        const resp_string = try self.allocator.alloc(u8, length);

        const string_line = lines.next().?;
        @memcpy(resp_string, string_line);

        return .{
            .bulk_string = resp_string,
        };
    }

    pub fn parse(self: *Self, lines: *U8SequenceIterator) error{ Overflow, InvalidCharacter, OutOfMemory }!RespData {
        const peeked_line = lines.peek().?;
        switch (peeked_line[0]) {
            '*' => {
                return try self.parseArray(lines);
            },
            '$' => {
                return try self.parseBulkString(lines);
            },
            else => {
                return .null;
            },
        }
    }

    pub fn readStream(self: *Self, reader: anytype) !?RespData {
        var bytes = std.ArrayList(u8).init(self.allocator);
        defer bytes.deinit();

        var buffer: [1024]u8 = undefined;

        while (true) {
            const bytes_read = reader.read(&buffer) catch |err| {
                switch (err) {
                    error.ConnectionResetByPeer => return null,
                    else => return err,
                }
            };
            if (bytes_read == 0) break;

            try bytes.appendSlice(buffer[0..bytes_read]);
            buffer = std.mem.zeroes([1024]u8);
            if (bytes_read < buffer.len) break;
        }
        if (bytes.items.len == 0) return null;

        var lines = std.mem.tokenizeSequence(u8, bytes.items, "\r\n");

        return try self.parse(&lines);
    }
};

test "RespParser read stream" {
    const data = "$4\r\nPING\r\n";
    const allocator = std.testing.allocator;

    var fbs = std.io.fixedBufferStream(data);
    const reader = fbs.reader();

    var parser = RespParser.init(allocator);

    const parsed_stream = try parser.readStream(reader);
    std.debug.assert(parsed_stream != null);

    var resp_data = parsed_stream.?;

    const expected = RespData{ .bulk_string = "PING" };
    try std.testing.expectEqualDeep(expected, resp_data);

    resp_data.deinit(allocator);
}

test "RespParser bulk string" {
    const data = "$4\r\nPING\r\n";
    const allocator = std.testing.allocator;

    var parser = RespParser.init(allocator);

    var lines = std.mem.tokenizeSequence(u8, data, "\r\n");

    var resp_data = try parser.parseBulkString(&lines);

    const expected = RespData{ .bulk_string = "PING" };
    try std.testing.expectEqualDeep(expected, resp_data);

    resp_data.deinit(allocator);
}

test "RespParser array" {
    const data = "*1\r\n$4\r\nPING\r\n";
    const allocator = std.testing.allocator;

    var parser = RespParser.init(allocator);

    var lines = std.mem.tokenizeSequence(u8, data, "\r\n");

    var resp_data = try parser.parseArray(&lines);

    var array = [_]RespData{RespData{ .bulk_string = "PING" }};

    const expected = RespData{ .array = &array };
    try std.testing.expectEqualDeep(expected, resp_data);

    resp_data.deinit(allocator);
}
