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

/** SQL Expression */
interface SqlExpr

/** Simple SQL identifier such as a table or column name */
data class SqlIdentifier(val id: String) : SqlExpr {
  override fun toString() = id
}

/** Binary expression */
data class SqlBinaryExpr(val l: SqlExpr, val op: String, val r: SqlExpr) : SqlExpr {
  override fun toString(): String = "$l $op $r"
}

/** SQL literal string */
data class SqlString(val value: String) : SqlExpr {
  override fun toString() = "'$value'"
}

/** SQL literal long */
data class SqlLong(val value: Long) : SqlExpr {
  override fun toString() = "$value"
}

/** SQL literal double */
data class SqlDouble(val value: Double) : SqlExpr {
  override fun toString() = "$value"
}

/** SQL function call */
data class SqlFunction(val id: String, val args: List<SqlExpr>) : SqlExpr {
  override fun toString() = id
}

/** SQL aliased expression */
data class SqlAlias(val expr: SqlExpr, val alias: SqlIdentifier) : SqlExpr

data class SqlCast(val expr: SqlExpr, val dataType: SqlIdentifier) : SqlExpr

data class SqlSort(val expr: SqlExpr, val asc: String) : SqlExpr

interface SqlRelation : SqlExpr

data class SqlSelect(
    val projection: List<SqlExpr>,
    val selection: SqlExpr?,
    val groupBy: List<SqlExpr>,
    val orderBy: List<SqlExpr>,
    val tableName: String
) : SqlRelation
