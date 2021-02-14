# Configuration 
The rust executor and scheduler can be configured using toml files, environment variables and command line arguments. The specification for config options can be found in `rust/ballista/src/bin/[executor|scheduler]_config_spec.toml`. 

Those files fully define Ballista's configuration. If there is a discrepancy between this documentation and the files, assume those files are correct.

To get a list of command line arguments, run the binary with `--help`

There is an example config file at `ballista/rust/ballista/examples/example_executor_config.toml`

The order of precedence for arguments is: default config file < environment variables < specified config file < command line arguments. 

The executor and scheduler will look for the default config file at `/etc/ballista/[executor|scheduler].toml` To specify a config file use the `--config-file` argument. 

Environment variables are prefixed by `BALLISTA_EXECUTOR` or `BALLISTA_SCHEDULER` for the executor and scheduler respectively. Hyphens in command line arguments become underscores. For example, the `--scheduler-host` argument for the executor becomes `BALLISTA_EXECUTOR_SCHEDULER_HOST`