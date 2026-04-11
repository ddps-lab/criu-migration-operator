#!/bin/bash
# Generate self-signed TLS certificates for the webhook server.
# For production, use cert-manager instead.

set -euo pipefail

NAMESPACE="migration-system"
SERVICE="migration-webhook"
SECRET_NAME="migration-webhook-tls"
TMPDIR=$(mktemp -d)

# Generate CA
openssl genrsa -out "${TMPDIR}/ca.key" 2048
openssl req -x509 -new -nodes -key "${TMPDIR}/ca.key" \
  -subj "/CN=migration-webhook-ca" -days 3650 \
  -out "${TMPDIR}/ca.crt"

# Generate server key and CSR
openssl genrsa -out "${TMPDIR}/tls.key" 2048
openssl req -new -key "${TMPDIR}/tls.key" \
  -subj "/CN=${SERVICE}.${NAMESPACE}.svc" \
  -out "${TMPDIR}/server.csr" \
  -config <(cat <<EOF
[req]
req_extensions = v3_req
distinguished_name = req_distinguished_name
[req_distinguished_name]
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE}
DNS.2 = ${SERVICE}.${NAMESPACE}
DNS.3 = ${SERVICE}.${NAMESPACE}.svc
DNS.4 = ${SERVICE}.${NAMESPACE}.svc.cluster.local
EOF
)

# Sign server certificate
openssl x509 -req -in "${TMPDIR}/server.csr" \
  -CA "${TMPDIR}/ca.crt" -CAkey "${TMPDIR}/ca.key" \
  -CAcreateserial -out "${TMPDIR}/tls.crt" -days 3650 \
  -extensions v3_req \
  -extfile <(cat <<EOF
[v3_req]
subjectAltName = @alt_names
[alt_names]
DNS.1 = ${SERVICE}
DNS.2 = ${SERVICE}.${NAMESPACE}
DNS.3 = ${SERVICE}.${NAMESPACE}.svc
DNS.4 = ${SERVICE}.${NAMESPACE}.svc.cluster.local
EOF
)

# Create or update the TLS secret
kubectl -n "${NAMESPACE}" delete secret "${SECRET_NAME}" 2>/dev/null || true
kubectl -n "${NAMESPACE}" create secret tls "${SECRET_NAME}" \
  --cert="${TMPDIR}/tls.crt" \
  --key="${TMPDIR}/tls.key"

# Patch the MutatingWebhookConfiguration with CA bundle
CA_BUNDLE=$(base64 -w0 < "${TMPDIR}/ca.crt")
kubectl patch mutatingwebhookconfiguration migration-pod-injector \
  --type='json' \
  -p="[{\"op\": \"replace\", \"path\": \"/webhooks/0/clientConfig/caBundle\", \"value\": \"${CA_BUNDLE}\"}]"

echo "Certificates generated and applied."
echo "  Secret: ${NAMESPACE}/${SECRET_NAME}"
echo "  CA Bundle patched into MutatingWebhookConfiguration"

rm -rf "${TMPDIR}"
