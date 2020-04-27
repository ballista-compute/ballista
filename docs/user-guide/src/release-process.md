# Release Process

## Publishing JVM Artifacts

Java artifacts are published to Maven central.

```bash
./gradlew clean
./gradlew publish
```

## GPG Notes

https://help.github.com/en/github/authenticating-to-github/generating-a-new-gpg-key

```bash
gpg --full-generate-key
gpg --export-secret-keys > ~/.gnupg/secring.gpg
gpg --key-server keys.openpgp.org --send-keys KEYID
```