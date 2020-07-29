# Setting up a JVM development environment

## Install the Google Protocol Buffer compiler

The gradle build script uses the [protobuf-gradle-plugin](https://github.com/google/protobuf-gradle-plugin) Gradle 
plugin to generate Java source code from the Ballista protobuf file and this depends on the protobuf compiler being 
installed.

Use the following instructions to install the protobuf compiler on Ubuntu or similar Linux platforms. 

```bash
wget https://github.com/protocolbuffers/protobuf/releases/download/v3.11.4/protobuf-all-3.11.4.tar.gz
tar xzf protobuf-all-3.11.4.tar.gz
cd protobuf-all-3.11.4
./configure
make
sudo make install
sudo ldconfig
```
