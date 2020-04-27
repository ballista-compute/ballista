# Ballista Functionality

This page documents which logical operators and expressions are currently supported by the various executors.

## Operators

| Feature    | Rust  | JVM   | Spark |
|------------|-------|-------|-------|
|Projection  | Yes   | Yes   | Yes   |
|Selection   | Yes   | Yes   | Yes   |
|Aggregate   | Yes   | Yes   | Yes   |
|Sort  |    |    |    |
|Limit  |    |    |    |
|Offset  |    |    |    |
|Left Join  |    |    |    |
|Right Join  |    |    |    |

## Expressions

| Expression                                       | Rust | JVM  | Spark |
| ------------------------------------------------ | ---- | ---- | ----- |
| Column                                           | Yes  | Yes  | Yes   |
| Arithmetic Expressions (`+`, `-`, `*`, `/`, `%`) | Yes  | Yes  | Yes   |
| Simple aggregates (min, max, sum, avg)           | Yes  | Yes  | Yes   |
| Literal values (double, long, string)            | Yes  | Yes  | Yes   |
| CAST(expr AS type)                               | Yes  | Yes  | Yes   |
| Comparison Operators (=, !=, <, <=, >, >=)       | Yes  | Yes  | Yes   |
| Boolean operators (AND, OR, NOT)                 |      |      |       |
|                                                  |      |      |       |
