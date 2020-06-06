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

import org.ballistacompute.logical.LiteralString
import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun `tokenize simple SELECT`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("id"),
                CommaToken(),
                IdentifierToken("first_name"),
                CommaToken(),
                IdentifierToken("last_name"),
                KeywordToken("FROM"),
                IdentifierToken("employee")
        )
        val actual = tokenize("SELECT id, first_name, last_name FROM employee")
        assertEquals(expected, actual)
    }

    @Test
    fun `projection with binary expression`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("salary"),
                OperatorToken("*"),
                LiteralDoubleToken("0.1"),
                KeywordToken("FROM"),
                IdentifierToken("employee")
        )
        val actual = tokenize("SELECT salary * 0.1 FROM employee")
        assertEquals(expected, actual)
    }

    @Test
    fun `projection with aliased binary expression`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("salary"),
                OperatorToken("*"),
                LiteralDoubleToken("0.1"),
                KeywordToken("AS"),
                IdentifierToken("bonus"),
                KeywordToken("FROM"),
                IdentifierToken("employee")
        )
        val actual = tokenize("SELECT salary * 0.1 AS bonus FROM employee")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with WHERE`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                CommaToken(),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee"),
                KeywordToken("WHERE"),
                IdentifierToken("state"),
                OperatorToken("="),
                LiteralStringToken("CO")
        )
        val actual = tokenize("SELECT a, b FROM employee WHERE state = 'CO'")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with aggregates`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("state"),
                CommaToken(),
                IdentifierToken("MAX"),
                LParenToken(),
                IdentifierToken("salary"),
                RParenToken(),
                KeywordToken("FROM"),
                IdentifierToken("employee"),
                KeywordToken("GROUP"),
                KeywordToken("BY"),
                IdentifierToken("state")
        )
        val actual = tokenize("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize compound operators`() {
        val expected = listOf(
                IdentifierToken("a"),
                OperatorToken(">="),
                IdentifierToken("b"),
                KeywordToken("OR"),
                IdentifierToken("a"),
                OperatorToken("<="),
                IdentifierToken("b"),
                KeywordToken("OR"),
                IdentifierToken("a"),
                OperatorToken("<>"),
                IdentifierToken("b"),
                KeywordToken("OR"),
                IdentifierToken("a"),
                OperatorToken("!="),
                IdentifierToken("b")
        )
        val actual = tokenize("a >= b OR a <= b OR a <> b OR a != b")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize long values`() {
        val expected = listOf(
                LiteralLongToken("123456789"),
                OperatorToken("+"),
                LiteralLongToken("987654321")
        )
        val actual = tokenize("123456789 + 987654321")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize float double values`() {
        val expected = listOf(
                LiteralDoubleToken("123456789.00"),
                OperatorToken("+"),
                LiteralDoubleToken("987654321.001")
        )
        val actual = tokenize("123456789.00 + 987654321.001")
        assertEquals(expected, actual)
    }

    private fun tokenize(sql: String) : List<Token> {
        return SqlTokenizer(sql).tokenize().tokens
    }
}