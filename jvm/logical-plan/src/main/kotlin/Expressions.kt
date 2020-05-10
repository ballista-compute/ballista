package org.ballistacompute.logical

import org.apache.arrow.vector.types.pojo.ArrowType
import org.ballistacompute.datatypes.ArrowTypes
import org.ballistacompute.datatypes.Field
import java.sql.SQLException

/**
 * Logical expression representing a reference to a column by name.
 */
class Column(val name: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields.find { it.name == name } ?: throw SQLException("No column named '$name' in ${input.schema().fields.map { it.name }}")
    }

    override fun toString(): String {
        return "#$name"
    }

}

/** Convenience method to create a Column reference */
fun col(name: String) = Column(name)

/** Convenience method to create a Column reference */
fun max(expr: LogicalExpr) = Max(expr)

/**
 * Logical expression representing a reference to a column by index.
 */
class ColumnIndex(val i: Int): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return input.schema().fields[i]
    }

    override fun toString(): String {
        return "#$i"
    }

}

/**
 * Logical expression representing a literal string value.
 */
class LiteralString(val str: String): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field(str, ArrowTypes.StringType)
    }

    override fun toString(): String {
        return "'$str'"
    }

}

/** Convenience method to create a LiteralString */
fun lit(value: String) = LiteralString(value)

/**
 * Logical expression representing a literal long value.
 */
class LiteralLong(val n: Long): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.Int64Type)
    }

    override fun toString(): String {
        return n.toString()
    }

}

/** Convenience method to create a LiteralLong */
fun lit(value: Long) = LiteralLong(value)

/**
 * Logical expression representing a literal double value.
 */
class LiteralDouble(val n: Double): LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field(n.toString(), ArrowTypes.DoubleType)
    }

    override fun toString(): String {
        return n.toString()
    }

}

/** Convenience method to create a LiteralDouble */
fun lit(value: Double) = LiteralDouble(value)

class CastExpr(val expr: LogicalExpr, val dataType: ArrowType) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(expr.toField(input).name, dataType)
    }

    override fun toString(): String {
        return "CAST($expr AS $dataType)"
    }
}

fun cast(expr: LogicalExpr, dataType: ArrowType) = CastExpr(expr, dataType)

abstract class BinaryExpr(val name: String,
                          val op: String,
                          val l: LogicalExpr,
                          val r: LogicalExpr) : LogicalExpr {

    override fun toString(): String {
        return "$l $op $r"
    }
}

abstract class UnaryExpr(val name: String,
                          val op: String,
                          val expr: LogicalExpr) : LogicalExpr{

    override fun toString(): String {
        return "$op $expr"
    }
}

/** Logical expression representing a logical NOT */
class Not(expr: LogicalExpr): UnaryExpr("not", "NOT", expr) {

    override fun toField(input: LogicalPlan): Field {
        return Field(name, ArrowTypes.BooleanType)
    }

}

/** Binary expressions that return a boolean type */
abstract class BooleanBinaryExpr(name: String,
                                 op: String,
                                 l: LogicalExpr,
                                 r: LogicalExpr) : BinaryExpr(name, op, l, r) {

    override fun toField(input: LogicalPlan): Field {
        return Field(name, ArrowTypes.BooleanType)
    }

}

/** Logical expression representing a logical AND */
class And(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("and", "AND", l, r)

/** Logical expression representing a logical OR */
class Or(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("or", "OR", l, r)

/** Logical expression representing an equality (`=`) comparison */
class Eq(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("eq", "=", l, r)

/** Logical expression representing an inequality (`!=`) comparison */
class Neq(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("neq", "!=", l, r)

/** Logical expression representing a greater than (`>`) comparison */
class Gt(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("gt", ">", l, r)

/** Logical expression representing a greater than or equals (`>=`) comparison */
class GtEq(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("gteq", ">=", l, r)

/** Logical expression representing a less than (`<`) comparison */
class Lt(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("lt", "<", l, r)

/** Logical expression representing a less than or equals (`<=`) comparison */
class LtEq(l: LogicalExpr, r: LogicalExpr): BooleanBinaryExpr("lteq", "<=", l, r)

/** Convenience method to create an equality expression using an infix operator */
infix fun LogicalExpr.eq(rhs: LogicalExpr): LogicalExpr { return Eq(this, rhs) }

/** Convenience method to create an inequality expression using an infix operator */
infix fun LogicalExpr.neq(rhs: LogicalExpr): LogicalExpr { return Neq(this, rhs) }

/** Convenience method to create a greater than expression using an infix operator */
infix fun LogicalExpr.gt(rhs: LogicalExpr): LogicalExpr { return Gt(this, rhs) }

/** Convenience method to create a greater than or equals expression using an infix operator */
infix fun LogicalExpr.gteq(rhs: LogicalExpr): LogicalExpr { return GtEq(this, rhs) }

/** Convenience method to create a less than expression using an infix operator */
infix fun LogicalExpr.lt(rhs: LogicalExpr): LogicalExpr { return Lt(this, rhs) }

/** Convenience method to create a less than or equals expression using an infix operator */
infix fun LogicalExpr.lteq(rhs: LogicalExpr): LogicalExpr { return LtEq(this, rhs) }

abstract class MathExpr(name: String,
                        op: String,
                        l: LogicalExpr,
                        r: LogicalExpr) : BinaryExpr(name, op, l, r) {

    override fun toField(input: LogicalPlan): Field {
        return Field("mult", l.toField(input).dataType)
    }

}

class Add(l: LogicalExpr, r: LogicalExpr) : MathExpr("add", "+", l, r)
class Subtract(l: LogicalExpr, r: LogicalExpr) : MathExpr("subtract", "-", l, r)
class Multiply(l: LogicalExpr, r: LogicalExpr) : MathExpr("mult", "*", l, r)
class Divide(l: LogicalExpr, r: LogicalExpr) : MathExpr("div", "/", l, r)
class Modulus(l: LogicalExpr, r: LogicalExpr) : MathExpr("mod", "%", l, r)

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.add(rhs: LogicalExpr): LogicalExpr {
    return Add(this, rhs)
}

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.subtract(rhs: LogicalExpr): LogicalExpr {
    return Subtract(this, rhs)
}

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.mult(rhs: LogicalExpr): LogicalExpr {
    return Multiply(this, rhs)
}

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.div(rhs: LogicalExpr): LogicalExpr {
    return Divide(this, rhs)
}

/** Convenience method to create a multiplication expression using an infix operator */
infix fun LogicalExpr.mod(rhs: LogicalExpr): LogicalExpr {
    return Modulus(this, rhs)
}


/** Aliased expression e.g. `expr AS alias`. */
class Alias(val expr: LogicalExpr, val alias: String) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(alias, expr.toField(input).dataType)
    }

    override fun toString(): String {
        return "$expr as $alias"
    }
}

/** Convenience method to wrap the current expression in an alias using an infix operator */
infix fun LogicalExpr.alias(alias: String) : Alias {
    return Alias(this, alias)
}

/** Scalar function */
class ScalarFunction(val name: String, val args: List<LogicalExpr>, val returnType: ArrowType ) : LogicalExpr {
    override fun toField(input: LogicalPlan): Field {
        return Field(name, returnType)
    }

    override fun toString(): String {
        return "$name($args)"
    }
}

/** Base class for aggregate functions that are of the same type as the underlying expression */
abstract class AggregateExpr(val name: String, val expr: LogicalExpr) : LogicalExpr {

    override fun toField(input: LogicalPlan): Field {
        return Field(name, expr.toField(input).dataType)
    }

    override fun toString(): String {
        return "$name($expr)"
    }
}

/**
 * Logical expression representing the SUM aggregate expression.
 */
class Sum(input: LogicalExpr) : AggregateExpr("SUM", input)

/**
 * Logical expression representing the MIN aggregate expression.
 */
class Min(input: LogicalExpr) : AggregateExpr("MIN", input)

/**
 * Logical expression representing the MAX aggregate expression.
 */
class Max(input: LogicalExpr) : AggregateExpr("MAX", input)

/**
 * Logical expression representing the AVG aggregate expression.
 */
class Avg(input: LogicalExpr) : AggregateExpr("AVG", input)

/** Logical expression representing the COUNT aggregate expression. */
class Count(input: LogicalExpr) : AggregateExpr("COUNT", input) {

    override fun toField(input: LogicalPlan): Field {
        return Field("COUNT", ArrowTypes.Int32Type)
    }

    override fun toString(): String {
        return "COUNT($expr)"
    }
}

/** Logical expression representing the COUNT DISTINCT aggregate expression. */
class CountDistinct(input: LogicalExpr) : AggregateExpr("COUNT DISTINCT", input) {

    override fun toField(input: LogicalPlan): Field {
        return Field("COUNT_DISTINCT", ArrowType.Int(32, false))
    }

    override fun toString(): String {
        return "COUNT(DISTINCT $expr)"
    }
}
