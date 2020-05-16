#!/bin/bash

OLD_VERSION=0.2.5
NEW_VERSION=0.2.6-SNAPSHOT

set -e

find . -name Cargo.toml -exec sed -i -e "s/$OLD_VERSION/$NEW_VERSION/g" {} \;
find . -name build.gradle.kts -exec sed -i -e "s/$OLD_VERSION/$NEW_VERSION/g" {} \;
find . -name "*.dockerfile" -exec sed -i -e "s/:$OLD_VERSION/:$NEW_VERSION/g" {} \;