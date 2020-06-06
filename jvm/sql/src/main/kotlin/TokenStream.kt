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

class TokenStream(val tokens: List<Token>) {

    private val logger = Logger.getLogger(TokenStream::class.simpleName)

    var i = 0

    fun peek(): Token? {
        if (i < tokens.size) {
            return tokens[i]
        } else {
            return null
        }
    }

    fun next(): Token? {
        if (i < tokens.size) {
            return tokens[i++]
        } else {
            return null
        }
    }

    fun consumeKeywords(s: List<String>): Boolean {
        val save = i
        s.forEach { keyword ->
            if (!consumeKeyword(keyword)) {
                i = save
                return false
            }
        }
        return true
    }

    fun consumeKeyword(s: String): Boolean {
        val peek = peek()
        logger.fine("consumeKeyword('$s') next token is $peek")
        return if (peek == KeywordToken(s)) {
            i++
            logger.fine("consumeKeyword() returning true")
            true
        } else {
            logger.fine("consumeKeyword() returning false")
            false
        }
    }

    fun consumeToken(t: Token): Boolean {
        val peek = peek()
        return if (peek == t) {
            i++
            true
        } else {
            false
        }
    }

    override fun toString(): String {
        return tokens.withIndex().map { (index,token) ->
            if (index == i) {
                "*$token"
            } else {
                token.toString()
            }
        }.joinToString(" ")
    }
}