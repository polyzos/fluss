# Apache Fluss Helm Chart

This chart deploys an Apache Fluss cluster on Kubernetes, following Helm best practices and production‑grade defaults.

It creates:
- 1x Coordinator Server as a StatefulSet with a stable ClusterIP Service
- Nx Tablet Servers as a StatefulSet with a headless Service (stable per‑pod DNS)
- ConfigMaps for server.yaml (coordinator and tablet)
- Optional PersistentVolumes for data directories
- Optional ServiceAccount and PodDisruptionBudgets

Table of contents
- Requirements
- Quick start
- Architecture and DNS
- Configuration reference
  - Listeners and advertised.listeners
  - Security (SASL) and security.protocol.map
  - Zookeeper and storage (data.dir / remote.data.dir)
  - JVM options and logging
- Persistence
- Resource management
- NetworkPolicy (optional)
- Upgrades and rolling strategy
- Troubleshooting

## Requirements
- Kubernetes 1.22+
- Helm 3.8+
- ZooKeeper: included by default (zookeeper.enabled=true). Set zookeeper.enabled=false to use an external ensemble
- A container image for Fluss (defaults to apache/fluss:0.7)

## Quick start

1) Default (embedded ZooKeeper):

helm install fluss ./fluss-helm

2) External ZooKeeper:

helm install fluss ./fluss-helm \
  --set zookeeper.enabled=false \
  --set coordinator.config.zookeeper.address="my-zk:2181" \
  --set tablet.config.zookeeper.address="my-zk:2181"

3) Get the service endpoint for in‑cluster clients:

kubectl get svc fluss-fluss-coordinator -o jsonpath='{.spec.clusterIP}:{.spec.ports[0].port}'

For clients outside the cluster, see “advertised.listeners” below and consider an Ingress or a LoadBalancer service.

## Architecture and DNS
- Coordinator is exposed via a normal ClusterIP Service: <release>-fluss-coordinator
- Tablets are exposed via a headless Service: <release>-fluss-tablet-headless
- Tablet Pods get stable FQDNs: <pod-ordinal>.<headless-name>.<namespace>.svc

The chart injects HOST and advertised listener hostnames using per‑pod variables that Fluss’ docker entrypoint expands into server.yaml via envsubst.

## Configuration reference

The chart renders server.yaml from values into a ConfigMap and appends any overrideProperties via FLUSS_PROPERTIES at runtime (docker entrypoint appends and envsubsts).

Important Fluss options surfaced by the chart:
- zookeeper.address: Coordinator and Tablets point to your ZK ensemble.
- data.dir, remote.data.dir: Local persistent path for data; remote path for snapshots (OSS/HDFS). Tablets default to a PVC mounted at data.dir.
- bind.listeners: Where the server binds; typically use 0.0.0.0 inside Kubernetes.
- advertised.listeners: Externally advertised endpoints for clients and intra‑cluster communication. In K8s, advertise stable names:
  - Coordinator: CLIENT://${COORDINATOR_SERVICE_NAME}.${NAMESPACE}.svc:${COORDINATOR_SERVICE_PORT}
  - Tablet: CLIENT://${POD_FQDN}:${TABLET_SERVICE_PORT}
- internal.listener.name: Which listener is used for internal communication (defaults to INTERNAL).
- security.protocol.map: Map each listener to a protocol (e.g., INTERNAL:PLAINTEXT,CLIENT:SASL).
- security.sasl.enabled.mechanisms: Server‑side SASL mechanisms (e.g., plain).
- tablet-server.id: Required to be unique per TabletServer. The chart auto‑derives this from the StatefulSet pod ordinal at runtime.

Example excerpt (values.yaml):

coordinator:
  config:
    bind.listeners: "INTERNAL://0.0.0.0:0,CLIENT://0.0.0.0:9123"
    advertised.listeners: "CLIENT://${COORDINATOR_SERVICE_NAME}.${NAMESPACE}.svc:${COORDINATOR_SERVICE_PORT}"
    security.protocol.map: "INTERNAL:PLAINTEXT,CLIENT:PLAINTEXT"

tablet:
  config:
    bind.listeners: "INTERNAL://0.0.0.0:0,CLIENT://0.0.0.0:9123"
    advertised.listeners: "CLIENT://${POD_FQDN}:${TABLET_SERVICE_PORT}"
    security.protocol.map: "INTERNAL:PLAINTEXT,CLIENT:PLAINTEXT"

### Listeners and advertised.listeners
- bind.listeners defines actual binding addresses (use 0.0.0.0 inside pods).
- advertised.listeners must be resolvable by clients and peers. For tablets, we use the pod FQDN (headless service + ordinal). For the coordinator, we use the Service name.
- internal.listener.name selects which listener label is used for internal RPC.

### Security (SASL)
Fluss supports SASL. TLS/SSL options are not exposed in ConfigOptions at the time of writing; if TLS appears in future releases, add corresponding values and mount certificates accordingly.

To enable SASL (PLAIN) on the server side:
- Set security.protocol.map to map the CLIENT listener to SASL, e.g., CLIENT:SASL.
- Set security.sasl.enabled.mechanisms to include plain.
- Provide a JAAS configuration as a Kubernetes Secret and mount it, then set JVM option -Djava.security.auth.login.config to point to the mounted file. The chart supports:

coordinator.jaas.enabled=true
coordinator.jaas.existingSecret=my-jaas-secret
coordinator.jvmOpts=-Djava.security.auth.login.config=/opt/fluss/conf/jaas.conf

Similarly for tablet.jaas.

Client examples (based on Fluss client options):
- client.security.protocol=SASL
- client.security.sasl.mechanism=plain
- client.security.sasl.username=...
- client.security.sasl.password=...
- Or client.security.sasl.jaas.config=...

### Zookeeper and storage
- zookeeper.address must point to a reachable ensemble.
- data.dir defaults to /var/lib/fluss/data; tablets use a PVC if tablet.persistence.enabled=true.
- remote.data.dir can point to an object store (oss://) or HDFS (hdfs://) for snapshots; provide respective filesystem configuration via overrideProperties if needed (e.g., fs.oss.* keys).

### JVM options and logging
- Use coordinator.jvmOpts and tablet.jvmOpts to set common JVM flags.
- Logging configuration is bundled in the Fluss distribution; you can extend with extra volume mounts.

## Persistence
- coordinator.persistence.enabled=false by default; enable if you need to persist coordinator files.
- tablet.persistence.enabled=true by default; configure size and storageClass.

## Resource management
Set coordinator.resources and tablet.resources with requests/limits as appropriate for production.

## NetworkPolicy (optional)
Enable networkPolicy.enabled to create restrictive policies that only allow traffic from selected namespaces or additional ingress rules you define.

## Upgrades and rolling strategy
- StatefulSets roll pods one by one. Tablets derive ID from ordinal; scaling keeps IDs stable. If you shrink replicas, ensure data and routing are considered.

## Minikube: local quickstart with 3 tablet servers

The defaults already deploy 3 tablet servers (tablet.replicas=3). In Minikube, image pulling can fail if the default image is not available from your environment. The most reliable approach is to build the Fluss image locally and point the chart to it.

Option A: Use Minikube’s Docker daemon and a local image

- Start Minikube and point your shell to its Docker:

  eval $(minikube docker-env)

- Build Fluss distribution and Docker image from this repo:

  ./mvnw -T1C -DskipTests -Pdist clean package
  cp -r build-target docker/
  docker build -t fluss:0.7 docker

- Install the chart using the local image and prevent pulling from a registry:

  helm install fluss ./fluss-helm \
    --set image.repository=fluss \
    --set image.tag=0.7 \
    --set image.pullPolicy=Never

This will deploy:
- 1 coordinator
- 3 tablet servers
- Embedded ZooKeeper (default). If you prefer your own ZK, pass --set zookeeper.enabled=false and set the zookeeper.address values accordingly.

Option B: Use a private registry

- Tag and push the image to your registry, then set:

  helm install fluss ./fluss-helm \
    --set image.repository=REGISTRY/PROJECT/fluss \
    --set image.tag=0.7 \
    --set image.pullPolicy=IfNotPresent \
    --set image.pullSecrets[0].name=my-regcred

Verification

- Check pods:

  kubectl get pods -l app.kubernetes.io/instance=fluss

- Check services:

  kubectl get svc

- Port-forward the coordinator for local testing (if needed):

  kubectl port-forward svc/fluss-fluss-coordinator 9123:9123

Notes on advertised.listeners

- Coordinator advertised as: CLIENT://${COORDINATOR_SERVICE_NAME}.${NAMESPACE}.svc:${COORDINATOR_SERVICE_PORT}
- Tablets advertised as: CLIENT://${POD_FQDN}:${TABLET_SERVICE_PORT}

These point to in-cluster DNS names. For access from outside the cluster, consider a LoadBalancer/NodePort/Ingress and update advertised.listeners accordingly.

## Troubleshooting
- Image pull errors:
  - In Minikube, prefer building locally and use image.pullPolicy=Never (see above).
  - If using a private registry, configure image.pullSecrets and ensure the image repository/tag are correct.
- Pods not ready: ensure ZooKeeper is reachable and ports 9123 are open.
- Connection failures: check advertised.listeners configuration and DNS resolution for Service and Pod FQDNs.
- SASL issues: confirm JAAS files are mounted, JVM option is set, and security.protocol.map maps the listener to SASL.

