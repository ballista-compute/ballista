# Ballista Python Bindings

This is a placeholder for Python bindings for Ballista.

Python bindings already exist for DataFusion [1] so it should be possible to adapt this code to work with Ballista.

[1] https://pypi.org/project/datafusion/

Currently the easiest way to expirament with the bindings is to activate a virtual environment and then run maturin develop

This is currently not manylinux compliant due to ballista's k8s dependency which uses hyper-tls which relies on libcrypto.so.1.1 through openssl. Options are to use features on the main ballista library and make it an optional dependency for building executor/scheduler or to ask for feature to vendor openssl or use rust-tls feature of k8s crate.

Another option is to move bindings around so that the ballista lib dependency is strictly beneath the root path of the python bindings as relative parent paths are not allowed in source distributions. Another possiblity is to use build.rs script to copy the ballista files into a sub directory here or rely on publishing ballista to crates.io and pull it in like a normal dependency.

Currently to build wheels run, which builds wheels for available python3 installations into ../target/wheels/. 
```maturin build --skip-auditwheel --no-sdist```


### Bindings design
1. Question of API coverage and how much behavior to expose to python side. As an example how much of DFSchema's API should be exposed. Should you only be able to print a representation from the python side, should some methods be implemented, or should a bunch of conveineice protocols like iterators be added.
2. PyCell or return by value for CaseBuilder, currently CaseBuilder takes reference to self, allowing for cases to be built without extra allocations, is this worth worrying about or is the inability to trigger an aliasing exception in the allocating version worth the extra cost.
3. cargo make or shell scripts for automating multistep processes. cargo make with duckscript is more cross platform than .sh files but harder to read imo.

### todos
1. Return in memory datafusion dataframe to allow for seamless remote execution to local analysis.
2. Join method on BallistaDataframe
