# Contributing to Ballista

See https://github.com/andygrove/ballista/issues for a list of open issues.

# Before committing

To make sure your build passes the checks in Travis CI, run these commands before creating a pull request.

```
cargo fmt --all
cargo clippy --all
cargo test
```