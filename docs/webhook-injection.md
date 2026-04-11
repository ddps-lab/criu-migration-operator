# Pod Injection Webhook

Mutating Admission Webhook으로 기존 Deployment/Pod에 annotation 하나만 추가하면 CRIU migration agent sidecar가 자동 주입됩니다.

## Quick Start

### 1. Webhook 활성화

Controller 배포 시 `--enable-webhook` 플래그 추가:

```yaml
containers:
- name: controller
  args:
  - --enable-webhook
  - --webhook-cert-dir=/tmp/k8s-webhook-server/serving-certs
  ports:
  - containerPort: 9443
    name: webhook
  volumeMounts:
  - name: webhook-certs
    mountPath: /tmp/k8s-webhook-server/serving-certs
    readOnly: true
volumes:
- name: webhook-certs
  secret:
    secretName: migration-webhook-tls
```

### 2. Migration Config 생성

ConfigMap으로 migration 설정을 정의합니다:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: my-migration-config
  namespace: migration-system
data:
  storageType: "minio"
  bucket: "checkpoints"
  endpoint: "http://minio.minio.svc:9000"
  region: "us-east-1"
  credentialsSecret: "minio-credentials"
  strategy: "lazy-storage"
  checkpointInterval: "30s"
  directUpload: "true"
  asyncPrefetch: "true"
  prefetchWorkers: "4"
  logUpload: "true"
```

### 3. 기존 Deployment에 Annotation 추가

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: my-app
spec:
  template:
    metadata:
      annotations:
        migration.io/enabled: "true"
        migration.io/config: "my-migration-config"
    spec:
      containers:
      - name: app
        image: my-app:latest
```

이것만으로 자동으로:
1. criu-agent sidecar 주입
2. App container의 command를 `sleep infinity`로 교체 (원본 annotation 저장)
3. 공유 PID namespace, SYS_PTRACE capability 설정
4. MigratableApp CR 자동 생성
5. 주기적 pre-checkpoint 시작
6. Spot interrupt 시 자동 migration

## Annotations Reference

### 필수

| Annotation | 값 | 설명 |
|------------|-----|------|
| `migration.io/enabled` | `"true"` | Sidecar injection 활성화 |

### Config 선택

| Annotation | 기본값 | 설명 |
|------------|--------|------|
| `migration.io/config` | `migration-defaults` | ConfigMap 이름. Pod namespace → migration-system 순서로 조회 |

### Override (선택)

ConfigMap 값을 개별 Pod에서 override할 때 사용:

| Annotation | 예시 | 설명 |
|------------|------|------|
| `migration.io/bucket` | `"my-bucket"` | S3 bucket |
| `migration.io/endpoint` | `"http://minio:9000"` | S3 endpoint |
| `migration.io/strategy` | `"lazy-storage"` | Migration strategy |
| `migration.io/direct-upload` | `"true"` | CRIU S3 direct upload |
| `migration.io/async-prefetch` | `"true"` | Async prefetch |
| `migration.io/log-upload` | `"true"` | Raw log S3 upload |
| `migration.io/agent-image` | `"my-agent:v2"` | Agent image override |
| `migration.io/create-cr` | `"false"` | MigratableApp CR 생성 비활성화 |

### 동작 제어

| Annotation | 기본값 | 설명 |
|------------|--------|------|
| `migration.io/create-cr` | `"true"` | MigratableApp CR 자동 생성 여부 |
| `migration.io/app-name` | (auto) | App 이름 (미지정 시 Deployment 이름에서 추론) |

## ConfigMap 조회 순서

`migration.io/config: "my-config"` 지정 시:

1. Pod과 같은 namespace에서 `my-config` ConfigMap 조회
2. 없으면 `migration-system` namespace에서 조회
3. 둘 다 없으면 injection skip (에러 없이 Pod 정상 생성)

`migration.io/config` 미지정 시:
- `migration-system/migration-defaults` ConfigMap 사용
- 없으면 코드 내장 기본값 사용

## Controller 공존

Webhook은 기존 MigratableApp CRD 방식과 완전히 공존합니다:

| 방식 | Pod 생성 | Pod lifecycle | Checkpoint | Migration |
|------|---------|---------------|------------|-----------|
| MigratableApp CRD | Controller | Controller | Controller | Controller |
| Webhook injection | Deployment | Deployment | Controller | Controller |

Webhook이 생성한 MigratableApp CR에는 `migration.io/webhook-managed: "true"` label이 붙으며, Controller는 이 label을 감지하여:
- Pod 생성/삭제를 **skip** (Deployment가 관리)
- Checkpoint scheduling과 migration trigger는 **수행**

## Setup

### TLS 인증서

cert-manager가 있는 환경:
```yaml
apiVersion: cert-manager.io/v1
kind: Certificate
metadata:
  name: migration-webhook-cert
  namespace: migration-system
spec:
  secretName: migration-webhook-tls
  dnsNames:
    - migration-webhook.migration-system.svc
  issuerRef:
    name: selfsigned-issuer
    kind: ClusterIssuer
```

cert-manager가 없는 환경:
```bash
./config/webhook/generate-certs.sh
```

### Kubernetes 매니페스트

```bash
kubectl apply -f config/webhook/service.yaml      # Webhook Service
kubectl apply -f config/webhook/webhook.yaml       # MutatingWebhookConfiguration
kubectl apply -f config/webhook/defaults.yaml      # Default ConfigMap (선택)
```

## 로그 확인

### Webhook 처리 로그 (Controller)

```
[WEBHOOK] Intercepted pod default/ (generateName: my-app-6c9778b84-)
[WEBHOOK] App name: my-app, namespace: default
[WEBHOOK] Created MigratableApp default/my-app for pod injection
```

### Controller reconcile (webhook-managed)

```
Reconciling webhook-managed app  {"name": "my-app"}
Webhook-managed pre-checkpoint   {"reason": "interval"}
Pre-checkpoint completed         {"dumpID": "e3a156ea-...", "chainDepth": 1}
```

### Agent sidecar

```
Network bandwidth detected: source=on-premise, baseline=1250 MB/s
profiler: uffd-wp setup complete (pid=29, registered=4 VMAs)
profiler: started (pid=29, interval=5000ms, threshold=0.30, consecutive=3)
Profiler: 1 exclude ranges, 0 no-parent ranges
Direct upload mode: CRIU uploaded checkpoint to S3
Uploaded agent metadata: test-webhook/0/worker1/.../hot-vmas.json
[LOG_UPLOAD] Uploaded 2 log files for pre-dump
```

## Credentials Secret Mirroring

Webhook은 `credentialsSecret`에 지정된 secret을 `migration-system` namespace에서 Pod의 namespace로 자동 복사합니다:

```
[WEBHOOK] Mirrored credentials secret minio-credentials to namespace default
```

Secret은 `envFrom.secretRef`로 agent sidecar에 주입됩니다.

## Limitations

- Multi-replica Deployment: sidecar는 모든 Pod에 주입되지만, `migration.io/create-cr: "false"`를 설정하지 않으면 replicas 간 MigratableApp CR 충돌 가능. Single-replica workload에 최적화됨.
- `sleep infinity`를 포함하지 않는 base image (scratch 등)에서는 busybox 등 sleep을 지원하는 image 필요.
- Webhook `failurePolicy: Ignore`로 설정되어 webhook 장애 시 Pod는 sidecar 없이 정상 생성됨.
