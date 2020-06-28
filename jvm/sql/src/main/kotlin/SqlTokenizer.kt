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

import java.util.logging.Logger

class SqlTokenizer(val sql: String) {

  private val logger = Logger.getLogger(SqlTokenizer::class.simpleName)

  // TODO this whole class is pretty crude and needs a lot of attention + unit tests (Hint: this
  // would be a great
  // place to start contributing!)

  val keywords =
      listOf("SELECT", "FROM", "WHERE", "AND", "OR", "NOT", "GROUP", "ORDER", "BY", "AS", "CAST")

  var i = 0

  fun tokenize(): TokenStream {
    var token = nextToken()
    val list = mutableListOf<Token>()
    while (token != null) {
      list.add(token)
      token = nextToken()
    }
    return TokenStream(list)
  }

  private fun nextToken(): Token? {

    // skip whitespace
    while (i < sql.length && sql[i].isWhitespace()) {
      i++
    }

    // EOF check
    if (i >= sql.length) {
      return null
    }

    // look for start of token
    if (sql[i] == ',') {
      i++
      return CommaToken()
    } else if (sql[i] == '(') {
      i++
      return LParenToken()
    } else if (sql[i] == ')') {
      i++
      return RParenToken()
    } else if (isIdentifierStart(sql[i])) {
      val start = i
      while (i < sql.length && isIdentifierPart(sql[i])) {
        i++
      }
      val s = sql.substring(start, i)
      if (keywords.contains(s.toUpperCase())) {
        return KeywordToken(s.toUpperCase())
      } else {
        return IdentifierToken(s)
      }
    } else if (listOf('=', '*', '/', '%', '-', '+', '<', '>', '!').contains(sql[i])) {
      val start = i
      i++

      val compoundOperator = sql.substring(start, i + 1)
      if (listOf("<=", ">=", "<>", "!=").contains(compoundOperator)) {
        i++
        return OperatorToken(compoundOperator)
      }

      if (sql[start] == '!') {
        throw IllegalStateException(
            "Invalid character '${sql[start]}' at position $start in '$sql' expected: !=")
      }

      return OperatorToken(sql.substring(start, i))
    } else if (sql[i] == '\'') {
      // TODO handle escaped quotes in string
      i++
      val start = i
      while (i < sql.length && sql[i] != '\'') {
        i++
      }
      i++
      return LiteralStringToken(sql.substring(start, i - 1))
    } else if (sql[i].isDigit() || sql[i] == '.') {
      // TODO support floating point numbers correctly
      val start = i
      while (i < sql.length && (sql[i].isDigit() || sql[i] == '.')) {
        i++
      }
      val str = sql.substring(start, i)
      if (str.contains('.')) {
        return LiteralDoubleToken(str)
      } else {
        return LiteralLongToken(str)
      }
    } else {
      throw IllegalStateException("Invalid character '${sql[i]}' at position $i in '$sql'")
    }
  }

  private fun isIdentifierStart(ch: Char): Boolean {
    return ch.isLetter()
  }

  private fun isIdentifierPart(ch: Char): Boolean {
    return ch.isLetter() || ch.isDigit() || ch == '_'
  }
}
