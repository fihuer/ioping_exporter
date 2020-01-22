# ioping Exporter for Prometheus

This is a simple server that uses ioping processes to measure IO latency on several filesystem
and then exposes results in the Prometheus metric format. This is base on the HAProxy exporter

## Getting Started

To build it:
```bash
go build .
```
To run it:

```bash
./ioping_exporter home=$HOME shm=/dev/shm/
```

