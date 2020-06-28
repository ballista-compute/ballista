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

/** Pratt Top Down Operator Precedence Parser. See https://tdop.github.io/ for paper. */
interface PrattParser {

  /** Parse an expression */
  fun parse(precedence: Int = 0): SqlExpr? {
    var expr = parsePrefix() ?: return null
    while (precedence < nextPrecedence()) {
      expr = parseInfix(expr, nextPrecedence())
    }
    return expr
  }

  /** Get the precedence of the next token */
  fun nextPrecedence(): Int

  /** Parse the next prefix expression */
  fun parsePrefix(): SqlExpr?

  /** Parse the next infix expression */
  fun parseInfix(left: SqlExpr, precedence: Int): SqlExpr
}
