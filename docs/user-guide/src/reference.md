# Reference

This page documents which operators and expressions are currently supported by the various executor implementations.

## Operators

| Feature    | Rust  | JVM   | Spark |
|------------|-------|-------|-------|
|Projection  | Yes   | Yes   | Yes   |
|Selection   | Yes   | Yes   | Yes   |
|Aggregate   | Yes   | Yes   | Yes   |
|Sort        | No    | No    | No    |
|Limit       | No    | No    | No    |
|Offset      | No    | No    | No    |
|Join        | No    | No    | No    |

## Expressions

| Expression                                       | Rust | JVM  | Spark |
| ------------------------------------------------ | ---- | ---- | ----- |
| Column reference                                 | Yes  | Yes  | Yes   |
| Literal values (double, long, string)            | Yes  | Yes  | Yes   |
| Arithmetic Expressions (`+`, `-`, `*`, `/`)      | Yes  | Yes  | Yes   |
| Simple aggregates (min, max, sum, avg, count)    | Yes  | Yes  | Yes   |
| Comparison Operators (=, !=, <, <=, >, >=)       | Yes  | Yes  | Yes   |
| CAST(expr AS type)                               | No   | Yes  | No    |
| Boolean operators (AND, OR, NOT)                 | No   | Yes  | Yes   |
