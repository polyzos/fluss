# Examples for deploying Fluss with Datalake (Paimon)

This folder contains example values files and instructions to simplify installs in common environments.

See also: fluss-helm/examples/instruction.md for a step-by-step Minikube guide.

## Minikube (single-node) — values-minikube.yaml
- Uses Minikube's default StorageClass (usually `standard`) which is ReadWriteOnce (RWO).
- In a single-node Minikube cluster, all pods run on the same node, so a single RWO PVC can be mounted by multiple pods concurrently.
- Enables the shared Paimon warehouse so all pods can see the same tables.
- Enables tablet.externalAccess by default so outside-cluster clients can connect via simple port-forwarding.

Install:

1) Start Minikube and optionally build or load your image

   # If you want to build the project image locally inside Minikube's Docker
   eval $(minikube docker-env)
   ./mvnw -T1C -DskipTests -Pdist clean package
   cp -r build-target docker/
   docker build -t fluss/fluss:0.7.0 docker

   # Or ensure the default image in values.yaml is pullable in your environment

2) Install with the Minikube example values:

   helm install fluss ./fluss-helm -f fluss-helm/examples/values-minikube.yaml

3) Verify

   kubectl get pods -l app.kubernetes.io/instance=fluss
   kubectl get pvc

4) Connect (inside cluster)

   kubectl get svc fluss-fluss-coordinator -o jsonpath='{.spec.clusterIP}:{.spec.ports[0].port}'

5) If your client runs outside the cluster
- Since values-minikube.yaml enables tablet.externalAccess by default, you only need to port-forward each tablet pod (ordinal i) in separate terminals:

   kubectl port-forward pod/fluss-fluss-tablet-0 19000:9123
   kubectl port-forward pod/fluss-fluss-tablet-1 19001:9123
   kubectl port-forward pod/fluss-fluss-tablet-2 19002:9123

6) Create lake-enabled tables
- Lake storage is enabled per table. When creating a table, include:

   'table.datalake.enabled' = 'true'

Notes:
- The example requests a 20Gi PVC for the warehouse with accessMode ReadWriteOnce using the `standard` StorageClass.
- For multi-node or cloud clusters with RWX support (NFS/EFS/Azure Files/Filestore), prefer ReadWriteMany and set the appropriate storageClass.
