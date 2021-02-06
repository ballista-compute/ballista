# Release Process

These instructions are for project maintainers wishing to create public releases of Ballista.

## Build All Artifacts

Run the following command to build all Docker images.

```bash
./dev/build-all.sh
```

## Release Checklist

- [ ] Run TPC-H benchmarks
- [ ] Run examples

## Publishing Java artifacts to Maven Central

The JVM artifacts are published to Maven central by uploading to sonatype. You will need to set the environment 
variables `SONATYPE_USERNAME` and `SONATYPE_PASSWORD` to the correct values for your account and you will also need 
verified GPG keys available for signing the artifacts (instructions tbd).

Run the follow commands to publish the artifacts to a sonatype staging repository.

```bash
./dev/publish-jvm.sh
```

## Publishing Rust Artifacts

Run the following script to publish the Rust crate to crates.io.

```
./dev/publish-rust.sh
```

## Publishing Docker Images

Run the following script to publish the executor Docker images to Docker Hub.

```
./dev/publish-docker-images.sh
```

## GPG Notes

Refer to [this article](https://help.github.com/en/github/authenticating-to-github/generating-a-new-gpg-key) for 
instructions on setting up GPG keys. Some useful commands are:

```bash
gpg --full-generate-key
gpg --export-secret-keys > ~/.gnupg/secring.gpg
gpg --key-server keys.openpgp.org --send-keys KEYID
```