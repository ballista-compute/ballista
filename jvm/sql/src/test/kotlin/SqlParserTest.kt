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
class SqlParserTest {

  @Test
  fun `1 + 2 * 3`() {
    val expr = parse("1 + 2 * 3")
    val expected = SqlBinaryExpr(SqlLong(1), "+", SqlBinaryExpr(SqlLong(2), "*", SqlLong(3)))
    assertEquals(expected, expr)
  }

  @Test
  fun `1 * 2 + 3`() {
    val expr = parse("1 * 2 + 3")
    val expected = SqlBinaryExpr(SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)), "+", SqlLong(3))
    assertEquals(expected, expr)
  }

  @Test
  fun `simple SELECT`() {
    val select = parseSelect("SELECT id, first_name, last_name FROM employee")
    assertEquals("employee", select.tableName)
    assertEquals(
        listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")),
        select.projection)
  }

  @Test
  fun `projection with binary expression`() {
    val select = parseSelect("SELECT salary * 0.1 FROM employee")
    assertEquals("employee", select.tableName)
    assertEquals(
        listOf(SqlBinaryExpr(SqlIdentifier("salary"), "*", SqlDouble(0.1))), select.projection)
  }

  @Test
  fun `projection with aliased binary expression`() {
    val select = parseSelect("SELECT salary * 0.1 AS bonus FROM employee")
    assertEquals("employee", select.tableName)

    val expectedBinaryExpr = SqlBinaryExpr(SqlIdentifier("salary"), "*", SqlDouble(0.1))
    val expectedAliasedExpr = SqlAlias(expectedBinaryExpr, SqlIdentifier("bonus"))
    assertEquals(listOf(expectedAliasedExpr), select.projection)
  }

  @Test
  fun `parse SELECT with WHERE`() {
    val select = parseSelect("SELECT id, first_name, last_name FROM employee WHERE state = 'CO'")
    assertEquals(
        listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")),
        select.projection)
    assertEquals(SqlBinaryExpr(SqlIdentifier("state"), "=", SqlString("CO")), select.selection)
    assertEquals("employee", select.tableName)
  }

  @Test
  fun `parse SELECT with aggregates`() {
    val select = parseSelect("SELECT state, MAX(salary) FROM employee GROUP BY state")
    assertEquals(
        listOf(SqlIdentifier("state"), SqlFunction("MAX", listOf(SqlIdentifier("salary")))),
        select.projection)
    assertEquals(listOf(SqlIdentifier("state")), select.groupBy)
    assertEquals("employee", select.tableName)
  }

  @Test
  fun `parse SELECT with aliased aggregates`() {
    val select = parseSelect("SELECT state, MAX(salary) AS top_wage FROM employee GROUP BY state")
    val max = SqlFunction("MAX", listOf(SqlIdentifier("salary")))
    val alias = SqlAlias(max, SqlIdentifier("top_wage"))
    assertEquals(listOf(SqlIdentifier("state"), alias), select.projection)
    assertEquals(listOf(SqlIdentifier("state")), select.groupBy)
    assertEquals("employee", select.tableName)
  }

  @Test
  fun `parse SELECT with aggregates and CAST`() {
    val select =
        parseSelect("SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state")
    assertEquals(
        listOf(
            SqlIdentifier("state"),
            SqlFunction("MAX", listOf(SqlCast(SqlIdentifier("salary"), SqlIdentifier("double"))))),
        select.projection)
    assertEquals(listOf(SqlIdentifier("state")), select.groupBy)
    assertEquals("employee", select.tableName)
  }

  private fun parseSelect(sql: String): SqlSelect {
    return parse(sql) as SqlSelect
  }

  private fun parse(sql: String): SqlExpr? {
    println("parse() $sql")
    val tokens = SqlTokenizer(sql).tokenize()
    println(tokens)
    val parsedQuery = SqlParser(tokens).parse()
    println(parsedQuery)
    return parsedQuery
  }
}
