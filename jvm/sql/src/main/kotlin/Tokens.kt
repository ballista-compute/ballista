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

interface Token

data class IdentifierToken(val text: String) : Token {
    override fun toString(): String {
        return text
    }
}

abstract class TokenBase(val text: String) : Token {
    override fun toString(): String {
        return text
    }

    override fun hashCode(): Int {
        return this.toString().hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return this.toString() == other.toString()
    }
}

class LiteralStringToken(text: String) : TokenBase(text)
class LiteralLongToken(text: String) : TokenBase(text)
class LiteralDoubleToken(text: String) : TokenBase(text)
class KeywordToken(text: String) : TokenBase(text)
class OperatorToken(text: String) : TokenBase(text)

class CommaToken() : TokenBase(",")
class LParenToken() : TokenBase("(")
class RParenToken() : TokenBase(")")