
//#line 18 "/Users/nystrom/work/x10/cvs/p3/x10.compiler/src/x10/parser/x10.g"
//
// Licensed Material
// (C) Copyright IBM Corp, 2006
//

package x10.parser;

public interface X10Parsersym {
    public final static int
      TK_IntegerLiteral = 78,
      TK_LongLiteral = 79,
      TK_FloatingPointLiteral = 80,
      TK_DoubleLiteral = 81,
      TK_CharacterLiteral = 82,
      TK_StringLiteral = 83,
      TK_MINUS_MINUS = 85,
      TK_OR = 111,
      TK_MINUS = 90,
      TK_MINUS_EQUAL = 123,
      TK_NOT = 93,
      TK_NOT_EQUAL = 112,
      TK_REMAINDER = 116,
      TK_REMAINDER_EQUAL = 124,
      TK_AND = 113,
      TK_AND_AND = 117,
      TK_AND_EQUAL = 125,
      TK_LPAREN = 1,
      TK_RPAREN = 74,
      TK_MULTIPLY = 114,
      TK_MULTIPLY_EQUAL = 126,
      TK_COMMA = 84,
      TK_DOT = 91,
      TK_DIVIDE = 118,
      TK_DIVIDE_EQUAL = 127,
      TK_COLON = 96,
      TK_SEMICOLON = 75,
      TK_QUESTION = 128,
      TK_AT = 2,
      TK_LBRACKET = 73,
      TK_RBRACKET = 103,
      TK_XOR = 115,
      TK_XOR_EQUAL = 129,
      TK_LBRACE = 89,
      TK_OR_OR = 130,
      TK_OR_EQUAL = 131,
      TK_RBRACE = 98,
      TK_TWIDDLE = 94,
      TK_PLUS = 92,
      TK_PLUS_PLUS = 86,
      TK_PLUS_EQUAL = 132,
      TK_LESS = 107,
      TK_LEFT_SHIFT = 119,
      TK_LEFT_SHIFT_EQUAL = 133,
      TK_RIGHT_SHIFT = 120,
      TK_RIGHT_SHIFT_EQUAL = 134,
      TK_UNSIGNED_RIGHT_SHIFT = 121,
      TK_UNSIGNED_RIGHT_SHIFT_EQUAL = 135,
      TK_LESS_EQUAL = 108,
      TK_EQUAL = 97,
      TK_EQUAL_EQUAL = 100,
      TK_GREATER = 109,
      TK_GREATER_EQUAL = 110,
      TK_ELLIPSIS = 141,
      TK_RANGE = 138,
      TK_ARROW = 88,
      TK_DARROW = 122,
      TK_SUBTYPE = 104,
      TK_SUPERTYPE = 105,
      TK_abstract = 7,
      TK_as = 40,
      TK_assert = 49,
      TK_async = 50,
      TK_ateach = 41,
      TK_atomic = 23,
      TK_await = 51,
      TK_break = 52,
      TK_case = 29,
      TK_catch = 33,
      TK_class = 13,
      TK_clocked = 34,
      TK_const = 19,
      TK_continue = 53,
      TK_def = 15,
      TK_default = 30,
      TK_do = 42,
      TK_else = 43,
      TK_extends = 35,
      TK_extern = 24,
      TK_false = 54,
      TK_final = 8,
      TK_finally = 36,
      TK_finish = 55,
      TK_for = 44,
      TK_foreach = 45,
      TK_future = 99,
      TK_in = 106,
      TK_to = 136,
      TK_goto = 56,
      TK_has = 57,
      TK_here = 58,
      TK_if = 59,
      TK_implements = 46,
      TK_import = 37,
      TK_incomplete = 25,
      TK_instanceof = 31,
      TK_interface = 10,
      TK_local = 26,
      TK_native = 16,
      TK_new = 60,
      TK_next = 61,
      TK_nonblocking = 27,
      TK_now = 62,
      TK_null = 63,
      TK_or = 47,
      TK_package = 38,
      TK_private = 3,
      TK_property = 20,
      TK_protected = 4,
      TK_public = 5,
      TK_return = 64,
      TK_safe = 11,
      TK_self = 65,
      TK_sequential = 28,
      TK_shared = 32,
      TK_static = 6,
      TK_strictfp = 9,
      TK_super = 77,
      TK_switch = 66,
      TK_synchronized = 142,
      TK_this = 76,
      TK_throw = 67,
      TK_throws = 48,
      TK_transient = 17,
      TK_true = 68,
      TK_try = 69,
      TK_type = 14,
      TK_unsafe = 70,
      TK_val = 21,
      TK_value = 12,
      TK_var = 22,
      TK_volatile = 18,
      TK_when = 71,
      TK_while = 39,
      TK_EOF_TOKEN = 137,
      TK_IDENTIFIER = 72,
      TK_SlComment = 143,
      TK_MlComment = 144,
      TK_DocComment = 145,
      TK_ErrorId = 101,
      TK_PathType = 95,
      TK_any = 139,
      TK_current = 140,
      TK_SynchronizedStatement = 102,
      TK_ObjectKind = 146,
      TK_ArrayInitailizer = 87,
      TK_ERROR_TOKEN = 147;

      public final static String orderedTerminalSymbols[] = {
                 "",
                 "LPAREN",
                 "AT",
                 "private",
                 "protected",
                 "public",
                 "static",
                 "abstract",
                 "final",
                 "strictfp",
                 "interface",
                 "safe",
                 "value",
                 "class",
                 "type",
                 "def",
                 "native",
                 "transient",
                 "volatile",
                 "const",
                 "property",
                 "val",
                 "var",
                 "atomic",
                 "extern",
                 "incomplete",
                 "local",
                 "nonblocking",
                 "sequential",
                 "case",
                 "default",
                 "instanceof",
                 "shared",
                 "catch",
                 "clocked",
                 "extends",
                 "finally",
                 "import",
                 "package",
                 "while",
                 "as",
                 "ateach",
                 "do",
                 "else",
                 "for",
                 "foreach",
                 "implements",
                 "or",
                 "throws",
                 "assert",
                 "async",
                 "await",
                 "break",
                 "continue",
                 "false",
                 "finish",
                 "goto",
                 "has",
                 "here",
                 "if",
                 "new",
                 "next",
                 "now",
                 "null",
                 "return",
                 "self",
                 "switch",
                 "throw",
                 "true",
                 "try",
                 "unsafe",
                 "when",
                 "IDENTIFIER",
                 "LBRACKET",
                 "RPAREN",
                 "SEMICOLON",
                 "this",
                 "super",
                 "IntegerLiteral",
                 "LongLiteral",
                 "FloatingPointLiteral",
                 "DoubleLiteral",
                 "CharacterLiteral",
                 "StringLiteral",
                 "COMMA",
                 "MINUS_MINUS",
                 "PLUS_PLUS",
                 "ArrayInitailizer",
                 "ARROW",
                 "LBRACE",
                 "MINUS",
                 "DOT",
                 "PLUS",
                 "NOT",
                 "TWIDDLE",
                 "PathType",
                 "COLON",
                 "EQUAL",
                 "RBRACE",
                 "future",
                 "EQUAL_EQUAL",
                 "ErrorId",
                 "SynchronizedStatement",
                 "RBRACKET",
                 "SUBTYPE",
                 "SUPERTYPE",
                 "in",
                 "LESS",
                 "LESS_EQUAL",
                 "GREATER",
                 "GREATER_EQUAL",
                 "OR",
                 "NOT_EQUAL",
                 "AND",
                 "MULTIPLY",
                 "XOR",
                 "REMAINDER",
                 "AND_AND",
                 "DIVIDE",
                 "LEFT_SHIFT",
                 "RIGHT_SHIFT",
                 "UNSIGNED_RIGHT_SHIFT",
                 "DARROW",
                 "MINUS_EQUAL",
                 "REMAINDER_EQUAL",
                 "AND_EQUAL",
                 "MULTIPLY_EQUAL",
                 "DIVIDE_EQUAL",
                 "QUESTION",
                 "XOR_EQUAL",
                 "OR_OR",
                 "OR_EQUAL",
                 "PLUS_EQUAL",
                 "LEFT_SHIFT_EQUAL",
                 "RIGHT_SHIFT_EQUAL",
                 "UNSIGNED_RIGHT_SHIFT_EQUAL",
                 "to",
                 "EOF_TOKEN",
                 "RANGE",
                 "any",
                 "current",
                 "ELLIPSIS",
                 "synchronized",
                 "SlComment",
                 "MlComment",
                 "DocComment",
                 "ObjectKind",
                 "ERROR_TOKEN"
             };

    public final static boolean isValidForParser = true;
}
