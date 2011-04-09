/*
 *  This file is part of the X10 project (http://x10-lang.org).
 *
 *  This file is licensed to You under the Eclipse Public License (EPL);
 *  You may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *      http://www.opensource.org/licenses/eclipse-1.0.php
 *
 *  (C) Copyright IBM Corporation 2006-2010.
 */
/********************************************************************
 * WARNING!  THIS JAVA FILE IS AUTO-GENERATED FROM x10/parser/x10.g *
 ********************************************************************/

package x10.parser;

public interface X10Parsersym {
    public final static int
      TK_IntegerLiteral = 13,
      TK_LongLiteral = 14,
      TK_FloatingPointLiteral = 15,
      TK_DoubleLiteral = 16,
      TK_CharacterLiteral = 17,
      TK_StringLiteral = 18,
      TK_MINUS_MINUS = 32,
      TK_OR = 79,
      TK_MINUS = 30,
      TK_MINUS_EQUAL = 87,
      TK_NOT = 35,
      TK_NOT_EQUAL = 80,
      TK_REMAINDER = 70,
      TK_REMAINDER_EQUAL = 88,
      TK_AND = 81,
      TK_AND_AND = 86,
      TK_AND_EQUAL = 89,
      TK_LPAREN = 1,
      TK_RPAREN = 7,
      TK_MULTIPLY = 68,
      TK_MULTIPLY_EQUAL = 90,
      TK_COMMA = 11,
      TK_DOT = 34,
      TK_DIVIDE = 71,
      TK_DIVIDE_EQUAL = 91,
      TK_COLON = 46,
      TK_SEMICOLON = 9,
      TK_QUESTION = 101,
      TK_AT = 6,
      TK_LBRACKET = 2,
      TK_RBRACKET = 40,
      TK_XOR = 82,
      TK_XOR_EQUAL = 92,
      TK_LBRACE = 38,
      TK_OR_OR = 93,
      TK_OR_EQUAL = 94,
      TK_RBRACE = 41,
      TK_TWIDDLE = 36,
      TK_PLUS = 31,
      TK_PLUS_PLUS = 33,
      TK_PLUS_EQUAL = 95,
      TK_LESS = 72,
      TK_LEFT_SHIFT = 65,
      TK_LEFT_SHIFT_EQUAL = 96,
      TK_RIGHT_SHIFT = 66,
      TK_RIGHT_SHIFT_EQUAL = 97,
      TK_UNSIGNED_RIGHT_SHIFT = 67,
      TK_UNSIGNED_RIGHT_SHIFT_EQUAL = 98,
      TK_LESS_EQUAL = 73,
      TK_EQUAL = 39,
      TK_EQUAL_EQUAL = 61,
      TK_GREATER = 74,
      TK_GREATER_EQUAL = 75,
      TK_ELLIPSIS = 131,
      TK_RANGE = 76,
      TK_ARROW = 69,
      TK_DARROW = 107,
      TK_SUBTYPE = 47,
      TK_SUPERTYPE = 77,
      TK_abstract = 49,
      TK_as = 102,
      TK_assert = 117,
      TK_async = 108,
      TK_at = 43,
      TK_athome = 44,
      TK_ateach = 109,
      TK_atomic = 48,
      TK_break = 118,
      TK_case = 83,
      TK_catch = 110,
      TK_class = 50,
      TK_clocked = 45,
      TK_continue = 119,
      TK_def = 111,
      TK_default = 84,
      TK_do = 112,
      TK_else = 120,
      TK_extends = 113,
      TK_false = 19,
      TK_final = 51,
      TK_finally = 114,
      TK_finish = 42,
      TK_for = 115,
      TK_goto = 132,
      TK_haszero = 78,
      TK_here = 20,
      TK_if = 121,
      TK_implements = 122,
      TK_import = 85,
      TK_in = 103,
      TK_instanceof = 99,
      TK_interface = 104,
      TK_native = 52,
      TK_new = 10,
      TK_next = 3,
      TK_null = 21,
      TK_offer = 123,
      TK_offers = 124,
      TK_operator = 125,
      TK_package = 116,
      TK_private = 53,
      TK_property = 105,
      TK_protected = 54,
      TK_public = 55,
      TK_resume = 4,
      TK_return = 126,
      TK_self = 22,
      TK_static = 56,
      TK_struct = 63,
      TK_super = 12,
      TK_switch = 127,
      TK_this = 8,
      TK_throw = 128,
      TK_transient = 57,
      TK_true = 23,
      TK_try = 129,
      TK_type = 64,
      TK_val = 58,
      TK_var = 59,
      TK_acc = 60,
      TK_void = 37,
      TK_when = 130,
      TK_while = 106,
      TK_EOF_TOKEN = 100,
      TK_IDENTIFIER = 5,
      TK_SlComment = 133,
      TK_MlComment = 134,
      TK_DocComment = 135,
      TK_ByteLiteral = 24,
      TK_ShortLiteral = 25,
      TK_UnsignedIntegerLiteral = 26,
      TK_UnsignedLongLiteral = 27,
      TK_UnsignedByteLiteral = 28,
      TK_UnsignedShortLiteral = 29,
      TK_PseudoDoubleLiteral = 136,
      TK_ErrorId = 62,
      TK_ERROR_TOKEN = 137;

    public final static String orderedTerminalSymbols[] = {
                 "",
                 "LPAREN",
                 "LBRACKET",
                 "next",
                 "resume",
                 "IDENTIFIER",
                 "AT",
                 "RPAREN",
                 "this",
                 "SEMICOLON",
                 "new",
                 "COMMA",
                 "super",
                 "IntegerLiteral",
                 "LongLiteral",
                 "FloatingPointLiteral",
                 "DoubleLiteral",
                 "CharacterLiteral",
                 "StringLiteral",
                 "false",
                 "here",
                 "null",
                 "self",
                 "true",
                 "ByteLiteral",
                 "ShortLiteral",
                 "UnsignedIntegerLiteral",
                 "UnsignedLongLiteral",
                 "UnsignedByteLiteral",
                 "UnsignedShortLiteral",
                 "MINUS",
                 "PLUS",
                 "MINUS_MINUS",
                 "PLUS_PLUS",
                 "DOT",
                 "NOT",
                 "TWIDDLE",
                 "void",
                 "LBRACE",
                 "EQUAL",
                 "RBRACKET",
                 "RBRACE",
                 "finish",
                 "at",
                 "athome",
                 "clocked",
                 "COLON",
                 "SUBTYPE",
                 "atomic",
                 "abstract",
                 "class",
                 "final",
                 "native",
                 "private",
                 "protected",
                 "public",
                 "static",
                 "transient",
                 "val",
                 "var",
                 "acc",
                 "EQUAL_EQUAL",
                 "ErrorId",
                 "struct",
                 "type",
                 "LEFT_SHIFT",
                 "RIGHT_SHIFT",
                 "UNSIGNED_RIGHT_SHIFT",
                 "MULTIPLY",
                 "ARROW",
                 "REMAINDER",
                 "DIVIDE",
                 "LESS",
                 "LESS_EQUAL",
                 "GREATER",
                 "GREATER_EQUAL",
                 "RANGE",
                 "SUPERTYPE",
                 "haszero",
                 "OR",
                 "NOT_EQUAL",
                 "AND",
                 "XOR",
                 "case",
                 "default",
                 "import",
                 "AND_AND",
                 "MINUS_EQUAL",
                 "REMAINDER_EQUAL",
                 "AND_EQUAL",
                 "MULTIPLY_EQUAL",
                 "DIVIDE_EQUAL",
                 "XOR_EQUAL",
                 "OR_OR",
                 "OR_EQUAL",
                 "PLUS_EQUAL",
                 "LEFT_SHIFT_EQUAL",
                 "RIGHT_SHIFT_EQUAL",
                 "UNSIGNED_RIGHT_SHIFT_EQUAL",
                 "instanceof",
                 "EOF_TOKEN",
                 "QUESTION",
                 "as",
                 "in",
                 "interface",
                 "property",
                 "while",
                 "DARROW",
                 "async",
                 "ateach",
                 "catch",
                 "def",
                 "do",
                 "extends",
                 "finally",
                 "for",
                 "package",
                 "assert",
                 "break",
                 "continue",
                 "else",
                 "if",
                 "implements",
                 "offer",
                 "offers",
                 "operator",
                 "return",
                 "switch",
                 "throw",
                 "try",
                 "when",
                 "ELLIPSIS",
                 "goto",
                 "SlComment",
                 "MlComment",
                 "DocComment",
                 "PseudoDoubleLiteral",
                 "ERROR_TOKEN"
             };

    public final static int numTokenKinds = orderedTerminalSymbols.length;
    public final static boolean isValidForParser = true;
}
