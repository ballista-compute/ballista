# Ballista User Guide Source

This directory contains the sources for the user guide that is published at https://ballistacompute.org/docs/.

## Generate HTML

```bash
cargo install mdbook
mdbook build
```

## Deploy User Guide to Web Site

Requires ssh certificate to be available.

```bash
./deploy.sh
```