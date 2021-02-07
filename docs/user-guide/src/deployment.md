# Deployment

Ballista 0.4.0 will support the following cluster deployment modes:

- Local Mode: Single process containing scheduler and executor, intended for local development testing
- Standalone: Single scheduler process, supporting multiple executor processes that register with the scheduler
- Etcd: Scheduler uses [etcd](https://etcd.io/) as a backing support, so that multiple scheduler instances can run
  concurrently
- Kubernetes: Schedulers and executors will be deployed as stateful sets in [Kubernetes](https://kubernetes.io/)


