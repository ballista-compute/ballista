#!/bin/bash

OLD_VERSION=0.2.6-SNAPSHOT
NEW_VERSION=0.3.0-SNAPSHOT

set -e

find . -name Cargo.toml -exec sed -i -e "s/$OLD_VERSION/$NEW_VERSION/g" {} \;
find . -name build.gradle.kts -exec sed -i -e "s/$OLD_VERSION/$NEW_VERSION/g" {} \;
find . -name "*.dockerfile" -exec sed -i -e "s/:$OLD_VERSION/:$NEW_VERSION/g" {} \;
find . -name docker-compose.yml -exec sed -i -e "s/:$OLD_VERSION/:$NEW_VERSION/g" {} \;
find dev -name "*.sh" -exec sed -i -e "s/BALLISTA_VERSION=$OLD_VERSION/BALLISTA_VERSION=$NEW_VERSION/g" {} \;