package org.ballistacompute.sql

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlParserTest {

    @Test
    fun `1 + 2 * 3`() {
        val expr = parse("1 + 2 * 3")
        val expected = SqlBinaryExpr(SqlLong(1),
                "+",
                SqlBinaryExpr(SqlLong(2), "*", SqlLong(3))
        )
        assertEquals(expected, expr)
    }

    @Test
    fun `1 * 2 + 3`() {
        val expr = parse("1 * 2 + 3")
        val expected = SqlBinaryExpr(
                SqlBinaryExpr(SqlLong(1), "*", SqlLong(2)),
                "+",
                SqlLong(3)
        )
        assertEquals(expected, expr)
    }

    @Test
    fun `simple SELECT`() {
        val select = parseSelect("SELECT id, first_name, last_name FROM employee")
        assertEquals("employee", select.tableName)
        assertEquals(listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")), select.projection)
    }

    @Test
    fun `projection with binary expression`() {
        val select = parseSelect("SELECT salary * 0.1 FROM employee")
        assertEquals("employee", select.tableName)
        assertEquals(listOf(SqlBinaryExpr(SqlIdentifier("salary"), "*", SqlDouble(0.1))), select.projection)
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
        assertEquals(listOf(SqlIdentifier("id"), SqlIdentifier("first_name"), SqlIdentifier("last_name")), select.projection)
        assertEquals(SqlBinaryExpr(SqlIdentifier("state"), "=", SqlString("CO")), select.selection)
        assertEquals("employee", select.tableName)
    }

    @Test
    fun `parse SELECT with aggregates`() {
        val select = parseSelect("SELECT state, MAX(salary) FROM employee GROUP BY state")
        assertEquals(listOf(SqlIdentifier("state"), SqlFunction("MAX", listOf(SqlIdentifier("salary")))), select.projection)
        assertEquals(listOf(SqlIdentifier("state")), select.groupBy)
        assertEquals("employee", select.tableName)
    }

    @Test
    fun `parse SELECT with aggregates and CAST`() {
        val select = parseSelect("SELECT state, MAX(CAST(salary AS double)) FROM employee GROUP BY state")
        assertEquals(listOf(SqlIdentifier("state"), SqlFunction("MAX", listOf(SqlCast(SqlIdentifier("salary"), SqlIdentifier("double"))))), select.projection)
        assertEquals(listOf(SqlIdentifier("state")), select.groupBy)
        assertEquals("employee", select.tableName)
    }

    private fun parseSelect(sql: String) : SqlSelect {
        return parse(sql) as SqlSelect
    }

    private fun parse(sql: String) : SqlExpr? {
        println("parse() $sql")
        val tokens = SqlTokenizer(sql).tokenize()
        println(tokens)
        val parsedQuery = SqlParser(tokens).parse()
        println(parsedQuery)
        return parsedQuery
    }
}