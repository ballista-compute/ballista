# Deployment

Ballista 0.4.0 will support the following cluster deployment modes:

- [Local Mode](local-mode.md): Single process containing scheduler and executor, intended for local development testing
- [Standalone](standalone.md): Single scheduler process, supporting multiple executor processes that register with the scheduler
- [Etcd](standalone.md): Scheduler uses [etcd](https://etcd.io/) as a backing support, so that multiple scheduler instances can run
  concurrently
- [Kubernetes](kubernetes.md): Schedulers and executors will be deployed as stateful sets in [Kubernetes](https://kubernetes.io/)


