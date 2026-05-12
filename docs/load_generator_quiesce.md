# External load-generator quiesce before final dump

## Why

The current `redis_standalone.py` / `memcached_standalone.py` wrappers
spawn the server **and** a YCSB JVM client inside the same pod. When the
operator's spot-interrupt path fires `criu dump`, the JVM is still alive
in the dump's process tree and CRIU's freeze races against JVM thread
synchronisation / glibc malloc, producing SIGSEGV-like behaviour or
`Unable to interrupt task: <pid>` from compel. Killing the JVM "30 s
after start" inside the wrapper does not line up with the operator's
actual dump timing — the operator and the workload have no shared clock
for that handoff.

Container-correct pattern:

1. The `MigratableApp`'s pod runs **only the server** (`redis-server`,
   `memcached`, …) as PID 1. No JVM, no Python wrapper.
2. Load is driven from a **separate** Deployment / Job that connects to
   the server through a ClusterIP `Service`.
3. The operator, on detecting cordon (or any spot-interrupt signal),
   sends the load generator a graceful-stop signal **before** invoking
   `FinalDump`. The load generator closes its connections, the server's
   `ESTABLISHED` socket list shrinks, then CRIU dumps a cleaner tree.

## CRD changes

```go
// MigratableAppSpec
type PreDumpQuiesce struct {
    // Pod selector for load-generator pods to drain before dump.
    // Empty = no quiesce step.
    TargetPodSelector map[string]string `json:"targetPodSelector,omitempty"`
    // Max seconds to wait for the load generator to terminate after
    // SIGTERM. Default 5 s. After this we proceed with dump anyway.
    DrainSeconds *int32 `json:"drainSeconds,omitempty"`
    // SIGTERM (default) or scale-replicas-to-zero
    Action string `json:"action,omitempty"` // "sigterm" | "scale-zero"
}

type MigrationPolicy struct {
    // ...
    PreDumpQuiesce *PreDumpQuiesce `json:"preDumpQuiesce,omitempty"`
}
```

## Controller hook

In `pkg/controller/reconciler.go performMigration`, just before the
`FinalDump` gRPC call:

```go
if q := mapp.Spec.MigrationPolicy.PreDumpQuiesce; q != nil && q.TargetPodSelector != nil {
    if err := r.quiesceLoadGenerator(ctx, mapp, q); err != nil {
        logger.Info("load-generator quiesce failed, proceeding anyway", "err", err)
    }
}
// ... existing FinalDump call ...
```

`quiesceLoadGenerator` looks up pods matching the selector in the same
namespace and either:
- `Action == "sigterm"`: `kubectl exec <pod> -- kill -TERM 1` (PID 1
  inside the container) — or a Kubernetes API call to evict the pod.
- `Action == "scale-zero"`: find the pod's owner Deployment, patch
  `spec.replicas = 0`.

Then waits up to `DrainSeconds` for the pods to enter `Terminating` (or
disappear). The load generator must respond to SIGTERM by exiting its
read/write loop.

## Workload-pod side (one YCSB image, two CMDs)

```dockerfile
FROM eclipse-temurin:17-jre-jammy
RUN apt-get update && apt-get install -y curl python3 && \
    curl -L https://github.com/brianfrankcooper/YCSB/releases/download/0.17.0/ycsb-0.17.0.tar.gz | \
        tar xz -C /opt && ln -s /opt/ycsb-0.17.0 /opt/ycsb
COPY ycsb-wrapper.sh /usr/local/bin/ycsb-wrapper
ENTRYPOINT ["ycsb-wrapper"]
```

`ycsb-wrapper.sh`:

```sh
#!/bin/sh
# Run YCSB and exit cleanly on SIGTERM. Whatever YCSB produces on stdout
# is the load record.
set -e
trap 'kill -TERM $YCSB_PID 2>/dev/null; wait $YCSB_PID' TERM
/opt/ycsb/bin/ycsb.sh "$@" &
YCSB_PID=$!
wait $YCSB_PID
```

YCSB's JVM honours SIGTERM and shuts down threads/jdbc-conns within a
few seconds. That gives the operator a clean enough window.

## Example mapp pair (paper measurement layout)

```yaml
# redis-server only
apiVersion: migration.io/v1alpha1
kind: MigratableApp
metadata: {name: redis}
spec:
  migrationPolicy:
    strategy: lazy-storage
    autoMigrate: true
    preDumpQuiesce:
      targetPodSelector: {workload: redis-loadgen}
      drainSeconds: 5
      action: sigterm
  template:
    spec:
      containers:
      - name: app
        image: ...criu-workload:latest
        command: [sh,-c]
        args: ["exec redis-server --bind 0.0.0.0 --port 6379 --daemonize no"]
---
apiVersion: v1
kind: Service
metadata: {name: redis-svc}
spec:
  selector: {workload: redis}
  ports: [{port: 6379}]
---
apiVersion: apps/v1
kind: Deployment
metadata: {name: redis-loadgen}
spec:
  replicas: 1
  selector: {matchLabels: {workload: redis-loadgen}}
  template:
    metadata: {labels: {workload: redis-loadgen}}
    spec:
      containers:
      - name: ycsb
        image: ...ycsb:latest
        args: ["run", "redis", "-P", "/opt/ycsb/workloads/workloada",
               "-p", "redis.host=redis-svc", "-p", "redis.port=6379",
               "-p", "recordcount=5000000", "-threads", "16"]
```

## Open questions

- Should quiesce run only on spot-interrupt detection, or also on every
  manual `trigger=requested` migration? Probably both, gated by the
  presence of `preDumpQuiesce` in the spec.
- Does the load-generator pod need a readiness probe so the operator
  can wait for it to come back after a successful migration? Or is
  letting Kubernetes restart it from the Deployment's replica count
  good enough?
- For lazy-storage where the source process dies on FinalDump, the load
  generator can come back as soon as the new target pod is restored.
  For lazy-direct / lazy-hybrid (page-server modes), the load generator
  should stay quiesced until the source's page-server has shipped its
  pages (i.e. until the controller marks the mapp Running on the new
  node).
