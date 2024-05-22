const std = @import("std");

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

pub const RespType = union(enum) {
    string: []const u8,
    @"error": []const u8,
    integer: i64,
    strings: [][]const u8,
    array: []RespType,
    null,
    boolean: bool,
    double: f64,
    bigint: i128,
    errors: [][]const u8,
};
