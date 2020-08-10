// Copyright 2020 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.ballistacompute.sql

import kotlin.test.assertEquals
import org.junit.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun `tokenize simple SELECT`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("id", Literal.IDENTIFIER, 9),
                Token(",", Symbol.COMMA, 10),
                Token("first_name", Literal.IDENTIFIER, 21),
                Token(",", Symbol.COMMA, 22),
                Token("last_name", Literal.IDENTIFIER, 32),
                Token("FROM", Keyword.FROM, 37),
                Token("employee", Literal.IDENTIFIER, 46))
        val actual = SqlTokenizer("SELECT id, first_name, last_name FROM employee").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `projection with binary expression`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("salary", Literal.IDENTIFIER, 13),
                Token("*", Symbol.STAR, 15),
                Token("0.1", Literal.DOUBLE, 19),
                Token("FROM", Keyword.FROM, 24),
                Token("employee", Literal.IDENTIFIER, 33))
        val actual = SqlTokenizer("SELECT salary * 0.1 FROM employee").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `projection with aliased binary expression`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("salary", Literal.IDENTIFIER, 13),
                Token("*", Symbol.STAR, 15),
                Token("0.1", Literal.DOUBLE, 19),
                Token("AS", Keyword.AS, 22),
                Token("bonus", Literal.IDENTIFIER, 28),
                Token("FROM", Keyword.FROM, 33),
                Token("employee", Literal.IDENTIFIER, 42))
        val actual = SqlTokenizer("SELECT salary * 0.1 AS bonus FROM employee").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with WHERE`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("a", Literal.IDENTIFIER, 8),
                Token(",", Symbol.COMMA, 9),
                Token("b", Literal.IDENTIFIER, 11),
                Token("FROM", Keyword.FROM, 16),
                Token("employee", Literal.IDENTIFIER, 25),
                Token("WHERE", Keyword.WHERE, 31),
                Token("state", Literal.IDENTIFIER, 37),
                Token("=", Symbol.EQ, 39),
                Token("CO", Literal.STRING, 44))
        val actual = SqlTokenizer("SELECT a, b FROM employee WHERE state = 'CO'").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with aggregates`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("state", Literal.IDENTIFIER, 12),
                Token(",", Symbol.COMMA, 13),
                Token("MAX", Keyword.MAX, 17),
                Token("(", Symbol.LEFT_PAREN, 18),
                Token("salary", Literal.IDENTIFIER, 24),
                Token(")", Symbol.RIGHT_PAREN, 25),
                Token("FROM", Keyword.FROM, 30),
                Token("employee", Literal.IDENTIFIER, 39),
                Token("GROUP", Keyword.GROUP, 45),
                Token("BY", Keyword.BY, 48),
                Token("state", Literal.IDENTIFIER, 54))
        val actual = SqlTokenizer("SELECT state, MAX(salary) FROM employee GROUP BY state")
            .tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with aggregates and HAVING`() {
        val expected =
            listOf(
                Token("SELECT", Keyword.SELECT, 6),
                Token("state", Literal.IDENTIFIER, 12),
                Token(",", Symbol.COMMA, 13),
                Token("MAX", Keyword.MAX, 17),
                Token("(", Symbol.LEFT_PAREN, 18),
                Token("salary", Literal.IDENTIFIER, 24),
                Token(")", Symbol.RIGHT_PAREN, 25),
                Token("FROM", Keyword.FROM, 30),
                Token("employee", Literal.IDENTIFIER, 39),
                Token("GROUP", Keyword.GROUP, 45),
                Token("BY", Keyword.BY, 48),
                Token("state", Literal.IDENTIFIER, 54),
                Token("HAVING", Keyword.HAVING, 61),
                Token("MAX", Keyword.MAX, 65),
                Token("(", Symbol.LEFT_PAREN, 66),
                Token("salary", Literal.IDENTIFIER, 72),
                Token(")", Symbol.RIGHT_PAREN, 73),
                Token(">", Symbol.GT, 75),
                Token("10", Literal.LONG, 78))
        val actual = SqlTokenizer("SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10")
            .tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize compound operators`() {
        val expected =
            listOf(
                Token("a", Literal.IDENTIFIER, 1),
                Token(">=", Symbol.GT_EQ, 4),
                Token("b", Literal.IDENTIFIER, 6),
                Token("OR", Keyword.OR, 9),
                Token("a", Literal.IDENTIFIER, 11),
                Token("<=", Symbol.LT_EQ, 14),
                Token("b", Literal.IDENTIFIER, 16),
                Token("OR", Keyword.OR, 19),
                Token("a", Literal.IDENTIFIER, 21),
                Token("<>", Symbol.LT_GT, 24),
                Token("b", Literal.IDENTIFIER, 26),
                Token("OR", Keyword.OR, 29),
                Token("a", Literal.IDENTIFIER, 31),
                Token("!=", Symbol.BANG_EQ, 34),
                Token("b", Literal.IDENTIFIER, 36))
        val actual = SqlTokenizer("a >= b OR a <= b OR a <> b OR a != b").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize long values`() {
        val expected =
            listOf(
                Token("123456789", Literal.LONG, 9),
                Token("+", Symbol.PLUS, 11),
                Token("987654321", Literal.LONG, 21))
        val actual = SqlTokenizer("123456789 + 987654321").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize float double values`() {
        val expected =
            listOf(
                Token("123456789.00", Literal.DOUBLE, 12),
                Token("+", Symbol.PLUS, 14),
                Token("987654321.001", Literal.DOUBLE, 28))
        val actual = SqlTokenizer("123456789.00 + 987654321.001").tokenize().tokens
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize table group`() {
        val expected =
            listOf(
                Token("select", Keyword.SELECT, 6),
                Token("*", Symbol.STAR, 8),
                Token("from", Keyword.FROM, 13),
                Token("group", Literal.IDENTIFIER, 19)
            )
        val actual = SqlTokenizer("select * from group").tokenize().tokens
        assertEquals(expected, actual)
    }
}
