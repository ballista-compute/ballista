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


class SqlTokenizer(val sql: String) {

    // TODO this whole class is pretty crude and needs a lot of attention + unit tests (Hint: this
    // would be a great
    // place to start contributing!)

    var offset = 0

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
        offset = skipWhitespace(offset)
        var token: Token? = null
        when {
            offset >= sql.length -> {
                return token
            }
            Literal.isIdentifierStart(sql[offset]) -> {
                token = scanIdentifier(offset)
                offset = token.endOffset
            }
            Literal.isNumberStart(sql[offset]) -> {
                token = scanNumber(offset)
                offset = token.endOffset
            }
            Symbol.isSymbolStart(sql[offset]) -> {
                token = scanSymbol(offset)
                offset = token.endOffset
            }
            Literal.isCharsStart(sql[offset]) -> {
                token = scanChars(offset, sql[offset])
                offset = token.endOffset
            }
        }
        return token
    }

    /**
     * skip whitespace.
     * @return offset after whitespace skipped
     */
    private fun skipWhitespace(startOffset: Int): Int {
        return sql.indexOfFirst(startOffset) { ch -> !ch.isWhitespace() }
    }

    /**
     * scan number.
     *
     * @return number token
     */
    private fun scanNumber(startOffset: Int): Token {
        var endOffset = if ('-' == sql[startOffset]) {
            sql.indexOfFirst(startOffset + 1) { ch -> !ch.isDigit() }
        } else {
            sql.indexOfFirst(startOffset) { ch -> !ch.isDigit() }
        }
        if (endOffset == sql.length) {
            return Token(sql.substring(startOffset, endOffset), Literal.LONG, endOffset)
        }
        val isFloat = '.' == sql[endOffset]
        if (isFloat) {
            endOffset = sql.indexOfFirst(endOffset + 1) { ch -> !ch.isDigit() }
        }
        return Token(sql.substring(startOffset, endOffset), if (isFloat) Literal.DOUBLE else Literal.LONG, endOffset)
    }

    /**
     * scan identifier.
     *
     * @return identifier token
     */
    private fun scanIdentifier(startOffset: Int): Token {
        if ('`' == sql[startOffset]) {
            val endOffset: Int = getOffsetUntilTerminatedChar('`', startOffset)
            return Token(sql.substring(offset, endOffset), Literal.IDENTIFIER, endOffset)
        }
        val endOffset = sql.indexOfFirst(startOffset) { ch -> !Literal.isIdentifierPart(ch) }
        val text: String = sql.substring(startOffset, endOffset)
        return if (isAmbiguousIdentifier(text)) {
            Token(text, processAmbiguousIdentifier(endOffset, text), endOffset)
        } else {
            val tokenType: TokenType = Keyword.textOf(text) ?: Literal.IDENTIFIER
            Token(text, tokenType, endOffset)
        }
    }

    /**
     * table name: group / order
     * keyword: group by / order by
     *
     * @return
     */
    private fun isAmbiguousIdentifier(text: String): Boolean {
        return Keyword.ORDER.name.equals(text, true) || Keyword.GROUP.name.equals(text, true)
    }

    /**
     * process group by | order by
     */
    private fun processAmbiguousIdentifier(startOffset: Int, text: String): TokenType {
        val skipWhitespaceOffset = skipWhitespace(startOffset)
        return if (skipWhitespaceOffset != sql.length
            && Keyword.BY.name.equals(sql.substring(skipWhitespaceOffset, skipWhitespaceOffset + 2), true))
            Keyword.textOf(text)!! else Literal.IDENTIFIER
    }

    /**
     *  find another char's offset equals terminatedChar
     */
    private fun getOffsetUntilTerminatedChar(terminatedChar: Char, startOffset: Int): Int {
        val offset = sql.indexOf(terminatedChar, startOffset)
        return if (offset != -1) offset else
            throw TokenizeException("Must contain $terminatedChar in remain sql[$startOffset .. end]")
    }

    /**
     * scan symbol.
     *
     * @return symbol token
     */
    private fun scanSymbol(startOffset: Int): Token {
        var endOffset = sql.indexOfFirst(startOffset) { ch -> !Symbol.isSymbol(ch) }
        var text = sql.substring(offset, endOffset)
        var symbol: Symbol?
        while (null == Symbol.textOf(text).also { symbol = it }) {
            text = sql.substring(offset, --endOffset)
        }
        return Token(text, symbol ?: throw TokenizeException("$text Must be a Symbol!"), endOffset)
    }

    /**
     * scan chars like 'abc' or "abc"
     */
    private fun scanChars(startOffset: Int, terminatedChar: Char): Token {
        val endOffset = getOffsetUntilTerminatedChar(terminatedChar, startOffset + 1)
        return Token(sql.substring(startOffset + 1, endOffset), Literal.STRING, endOffset + 1)
    }

    private inline fun CharSequence.indexOfFirst(startIndex: Int = 0, predicate: (Char) -> Boolean): Int {
        for (index in startIndex until this.length) {
            if (predicate(this[index])) {
                return index
            }
        }
        return sql.length
    }
}

class TokenizeException(val msg: String) : Throwable()
