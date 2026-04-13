#!/bin/bash
# ==============================================================================
# 🛠️ Ray GCS High Availability & Failover Stress Test Framework
# ==============================================================================
# 
# 📌 PURPOSE:
#   This script facilitates complete end-to-end testing of the Ray GCS Shadow Head
#   architecture. It manages cluster lifecycles, injects targeted pod disruptions,
#   and observes cluster state continuity without worker restarts.
#
# ⚙️ USAGE COMMANDS:
#   bash run_failover_experiment.sh [FLAGS]
#
# 💡 AVAILABLE FLAGS:
#   --build-ray       : Builds the custom Ray C++ container (observer mode updates)
#   --build-kuberay   : Builds the updated KubeRay operator
#   --build-all       : Combined trigger for Ray & KubeRay builds
#   --create-cluster  : Bootstraps the base GKE cluster setup natively
#   --cleanup         : Purges lingering active cluster deployment boundaries cleanly
#
# ==============================================================================
set -e

LOG_FILE="/usr/local/google/home/yczhou/.gemini/jetski/brain/7012d1c5-9dc9-4876-aba5-7344a3c088b3/scratch/results_gcs_logging.log"
CLUSTER_NAME="ray-failover-cluster"
PF_PID=""

rm -f $LOG_FILE

log_msg() {
    echo "[RAY-FAILOVER-BENCHMARK] [$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a $LOG_FILE
}

global_cleanup() {
    if [ -n "$PF_PID" ]; then
        log_msg "[$(date '+%H:%M:%S')] 🧹 Forcefully terminating background port-forward tunnel (PID: $PF_PID)..."
        kill $PF_PID >/dev/null 2>&1 || true
    fi
}
trap global_cleanup EXIT

BUILD_RAY_FLAG=false
BUILD_KUBERAY_FLAG=false
CLEANUP_FLAG=false
CREATE_CLUSTER_FLAG=false

for arg in "$@"; do
    if [ "$arg" == "--build-ray" ] || [ "$arg" == "--build-all" ]; then
        BUILD_RAY_FLAG=true
    fi
    if [ "$arg" == "--build-kuberay" ] || [ "$arg" == "--build-all" ]; then
        BUILD_KUBERAY_FLAG=true
    fi
    if [ "$arg" == "--cleanup" ]; then
        CLEANUP_FLAG=true
    fi
    if [ "$arg" == "--create-cluster" ]; then
        CREATE_CLUSTER_FLAG=true
    fi
done

if [ "$CREATE_CLUSTER_FLAG" == true ]; then
    log_msg "====================================================================="
    log_msg "🚀 Phase 0: GKE Infrastructure Cluster Creation Pipeline"
    log_msg "====================================================================="

    log_msg "[$(date '+%H:%M:%S')] ☁️ Provisioning standard high-performance GKE cluster ($CLUSTER_NAME)..."
    gcloud container clusters create $CLUSTER_NAME \
        --zone=us-central1-c \
        --num-nodes=3 \
        --machine-type=e2-standard-8 \
        --scopes=https://www.googleapis.com/auth/cloud-platform
        
    log_msg "[$(date '+%H:%M:%S')] ☁️ Retrieving direct access credentials for kubectl context..."
    gcloud container clusters get-credentials $CLUSTER_NAME --zone=us-central1-c
    log_msg "[$(date '+%H:%M:%S')] ✅ GKE cluster successfully provisioned and accessible!"

    log_msg "[$(date '+%H:%M:%S')] 📜 Applying custom Ray CRDs to the new cluster..."
    kubectl apply -f /usr/local/google/home/yczhou/Projects/ray/kuberay/ray-operator/config/crd/bases/

    log_msg "[$(date '+%H:%M:%S')] 📦 Deploying custom KubeRay operator via standard Helm chart..."
    /usr/local/google/home/yczhou/Projects/ray/kuberay/ray-operator/bin/helm upgrade --install kuberay-operator /usr/local/google/home/yczhou/Projects/ray/kuberay/helm-chart/kuberay-operator \
      --namespace default \
      --set image.repository=us-central1-docker.pkg.dev/yczhou-gke-dev/shadow-head-repo/kuberay-operator \
      --set image.tag=latest \
      --set image.pullPolicy=Always

    log_msg "[$(date '+%H:%M:%S')] ⏳ Waiting for new KubeRay operator rollout to finish..."
    kubectl rollout status deployment/kuberay-operator -n default --timeout=120s
else
    log_msg "ℹ️ Phase 0 GKE cluster creation skipped (use --create-cluster to deploy a completely new cloud infrastructure)."
fi

if [ "$BUILD_RAY_FLAG" == true ] || [ "$BUILD_KUBERAY_FLAG" == true ]; then
    log_msg "====================================================================="
    log_msg "🚀 Phase 1: Granular Source Compilation & Artifact Deployment Pipeline"
    log_msg "====================================================================="

    if [ "$BUILD_RAY_FLAG" == true ]; then
        log_msg "[$(date '+%H:%M:%S')] 🐳 Executing self-contained C++ compilation for Ray shadow head..."
        docker build -t us-central1-docker.pkg.dev/yczhou-gke-dev/shadow-head-repo/ray-shadow-gcs:phase3-ttl-fix -f docker/ray/Dockerfile.shadow .
        
        log_msg "[$(date '+%H:%M:%S')] ☁️ Pushing custom Ray image to remote registry..."
        docker push us-central1-docker.pkg.dev/yczhou-gke-dev/shadow-head-repo/ray-shadow-gcs:phase3-ttl-fix
    else
        log_msg "ℹ️ Ray core shadow image compilation skipped."
    fi

    if [ "$BUILD_KUBERAY_FLAG" == true ]; then
        log_msg "[$(date '+%H:%M:%S')] 🐳 Building custom KubeRay operator image self-contained..."
        cd /usr/local/google/home/yczhou/Projects/ray/kuberay/ray-operator
        docker build -t us-central1-docker.pkg.dev/yczhou-gke-dev/shadow-head-repo/kuberay-operator:latest -f Dockerfile .
        
        log_msg "[$(date '+%H:%M:%S')] ☁️ Pushing custom KubeRay operator image..."
        docker push us-central1-docker.pkg.dev/yczhou-gke-dev/shadow-head-repo/kuberay-operator:latest

        log_msg "[$(date '+%H:%M:%S')] 🚀 Redeploying KubeRay operator directly to the cluster..."
        kubectl rollout restart deployment/kuberay-operator -n default
        kubectl rollout status deployment/kuberay-operator -n default --timeout=120s
    else
        log_msg "ℹ️ KubeRay Go operator image compilation skipped."
    fi

    log_msg "[$(date '+%H:%M:%S')] ✅ Phase 1 deployment configurations fully processed!"
else
    log_msg "ℹ️ Phase 1 compilation skipped entirely (use --build-ray, --build-kuberay, or --build-all)."
fi

if [ "$CLEANUP_FLAG" == true ]; then
    log_msg "====================================================================="
    log_msg "🚀 Phase 2: Global Infrastructure State Cleanup"
    log_msg "====================================================================="

    log_msg "[$(date '+%H:%M:%S')] 🧹 Terminating active RayCluster deployment immediately to trigger controller background removal..."
    kubectl delete -f /usr/local/google/home/yczhou/Projects/ray/ray-cluster-self-contained.yaml --ignore-not-found

    log_msg "[$(date '+%H:%M:%S')] 🧹 Deleting persistent Redis deployment and service..."
    kubectl delete deployment redis -n default --ignore-not-found
    kubectl delete svc redis -n default --ignore-not-found

    log_msg "[$(date '+%H:%M:%S')] 🧹 Deleting active GCS leader election Lease object..."
    kubectl delete lease ray-self-contained-gcs -n default --ignore-not-found

    log_msg "[$(date '+%H:%M:%S')] 🧹 Removing lingering KubeRay headless service definitions..."
    kubectl delete svc ray-self-contained-gcs-head-svc -n default --ignore-not-found

    log_msg "[$(date '+%H:%M:%S')] ⏳ Continuously polling until all associated Ray pods are completely terminated..."
    while true; do
        REMAINING_PODS=$(kubectl get pods -l ray.io/cluster=ray-self-contained-gcs -n default --no-headers 2>/dev/null | wc -l)
        if [ "$REMAINING_PODS" -eq 0 ]; then
            log_msg "[$(date '+%H:%M:%S')] ✅ All stale infrastructure and lingering containers completely removed!"
            break
        fi
        log_msg "[$(date '+%H:%M:%S')] ⏳ $REMAINING_PODS lingering pod(s) detected. Waiting 5s..."
        sleep 5
    done
else
    log_msg "ℹ️ Phase 2 global infrastructure cleanup skipped (use --cleanup to cleanly terminate all existing resources)."
fi

log_msg "====================================================================="
log_msg "🚀 Phase 3: Infrastructure Provisioning & Initialization Pipeline"
log_msg "====================================================================="

log_msg "[$(date '+%H:%M:%S')] 🛠️ Setting up cluster RBAC and core networking layers..."
kubectl apply -f - <<EOF
apiVersion: v1
kind: ServiceAccount
metadata:
  name: ray-user-sa
  namespace: default
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: ray-leader-election-role
  namespace: default
rules:
- apiGroups: ["coordination.k8s.io"]
  resources: ["leases"]
  verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
- apiGroups: [""]
  resources: ["pods"]
  verbs: ["get", "list", "watch"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: ray-leader-election-binding
  namespace: default
subjects:
- kind: ServiceAccount
  name: ray-user-sa
  namespace: default
roleRef:
  kind: Role
  name: ray-leader-election-role
  apiGroup: rbac.authorization.k8s.io
EOF
log_msg "[$(date '+%H:%M:%S')] ✅ RBAC configurations applied."

log_msg "[$(date '+%H:%M:%S')] 🛠️ Deploying highly available persistent Redis datastore..."
kubectl apply -f - <<EOF
apiVersion: apps/v1
kind: Deployment
metadata:
  name: redis
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: redis
  template:
    metadata:
      labels:
        app: redis
    spec:
      containers:
      - name: redis
        image: redis:7.0
        ports:
        - containerPort: 6379
---
apiVersion: v1
kind: Service
metadata:
  name: redis
  namespace: default
spec:
  ports:
  - port: 6379
    targetPort: 6379
  selector:
    app: redis
EOF

log_msg "[$(date '+%H:%M:%S')] ⏳ Waiting for Redis backend to reach Ready=True state..."
kubectl wait --for=condition=Available deployment/redis -n default --timeout=60s

log_msg "[$(date '+%H:%M:%S')] 🛠️ Applying standard RayCluster specification..."
kubectl apply -f /usr/local/google/home/yczhou/Projects/ray/ray-cluster-self-contained.yaml

log_msg "[$(date '+%H:%M:%S')] 🔍 Continuously polling until exactly 2 fully stable Head Pods and 1 Worker Pod reach Running state..."
while true; do
    SHADOW_COUNT=$(kubectl get pods -l ray.io/node-type=head -n default --field-selector=status.phase=Running | grep -c Running || true)
    WORKER_COUNT=$(kubectl get pods -l ray.io/node-type=worker -n default --field-selector=status.phase=Running | grep -c Running || true)
    if [ "$SHADOW_COUNT" -ge 2 ] && [ "$WORKER_COUNT" -ge 1 ]; then
        log_msg "[$(date '+%H:%M:%S')] ✅ Infrastructure completely stabilized!"
        break
    fi
    log_msg "[$(date '+%H:%M:%S')] ⏳ Detected $SHADOW_COUNT/2 heads and $WORKER_COUNT/1 workers. Waiting 10s..."
    sleep 10
done

ACTIVE_WORKER=$(kubectl get pods -l ray.io/node-type=worker -n default --field-selector=status.phase=Running -o name | sed 's/pod\///' | head -n 1)
INITIAL_RESTARTS=$(kubectl get pod $ACTIVE_WORKER -n default -o jsonpath='{.status.containerStatuses[0].restartCount}')
log_msg "[$(date '+%H:%M:%S')] 🛡️ Target tracking worker designated: $ACTIVE_WORKER (Initial restarts: $INITIAL_RESTARTS)"

log_msg "====================================================================="
log_msg "🚀 Phase 4: Baseline Data Seeding & Pre-Failover Workload Execution"
log_msg "====================================================================="

PRIMARY_POD=$(kubectl get lease -n default ray-self-contained-gcs -o jsonpath='{.spec.holderIdentity}')
log_msg "[$(date '+%H:%M:%S')] 👑 Initial primary leader verified: $PRIMARY_POD"

log_msg "[$(date '+%H:%M:%S')] 🛠️ Creating local demonstration directory and standard Python validation payload..."
mkdir -p /tmp/ray_job_demo
cat << 'EOF' > /tmp/ray_job_demo/script.py
import ray

print("Initializing Ray connection within submitted payload...")
ray.init()

@ray.remote
def validation_task():
    return "RAY_JOB_SUBMISSION_COMPUTATION_SUCCESS"

result = ray.get(validation_task.remote())
print(f"Task Computation Output: {result}")
EOF

log_msg "[$(date '+%H:%M:%S')] 🔌 Establishing direct background port-forward to the load-balanced HA Kubernetes Head Service (svc/ray-self-contained-gcs-head-svc)..."
kubectl port-forward svc/ray-self-contained-gcs-head-svc 8265:8265 -n default > /dev/null 2>&1 &
PF_PID=$!

log_msg "[$(date '+%H:%M:%S')] ⏳ Waiting 15 seconds to allow HA service-level port-forward tunnel to completely register its TCP endpoints..."
sleep 15

log_msg "[$(date '+%H:%M:%S')] ⚙️ Activating local Python environment to execute standard CLI pre-failover job submission with exponential retry mechanics..."
source /usr/local/google/home/yczhou/ray/bin/activate

MAX_RETRIES=6
RETRY_COUNT=0
SUBMISSION_SUCCESS=false
SUBMISSION_OUT=""

while [ "$RETRY_COUNT" -lt "$MAX_RETRIES" ]; do
    log_msg "[$(date '+%H:%M:%S')] 🚀 Attempting CLI job submission (Attempt $((RETRY_COUNT + 1))/$MAX_RETRIES)..."
    if SUBMISSION_OUT=$(RAY_ADDRESS="http://localhost:8265" ray job submit --working-dir /tmp/ray_job_demo -- python script.py 2>&1); then
        SUBMISSION_SUCCESS=true
        break
    fi
    log_msg "[$(date '+%H:%M:%S')] ⚠️ Submission endpoint unavailable. Retrying in 10 seconds..."
    sleep 10
    ((RETRY_COUNT++))
done

if [ "$SUBMISSION_SUCCESS" == false ]; then
    log_msg "[$(date '+%H:%M:%S')] 🚨 CRITICAL: Completely failed to reach the HA Ray Dashboard API after $MAX_RETRIES attempts!"
    exit 1
fi

PRE_FAILOVER_JOB_ID=$(echo "$SUBMISSION_OUT" | grep -o "raysubmit_[a-zA-Z0-9]*" | head -n 1)

log_msg "[$(date '+%H:%M:%S')] ✅ Pre-failover benchmark workload completely processed and persistent state successfully established."
log_msg "[$(date '+%H:%M:%S')] 📌 Pre-failover Seed Workload Job ID assigned: $PRE_FAILOVER_JOB_ID"

ITERATION=1

log_msg "====================================================================="
log_msg "🚀 Phase 5: Continuous Automated Failover & State Consistency Validation"
log_msg "====================================================================="

while true; do
    log_msg "=================================================="
    log_msg "[$(date '+%H:%M:%S')] 🔄 --- Iteration $ITERATION ---"
    
    PRIMARY_POD=$(kubectl get lease -n default ray-self-contained-gcs -o jsonpath='{.spec.holderIdentity}')
    STANDBY_POD=$(kubectl get pods -l ray.io/node-type=head -n default --field-selector=status.phase=Running -o name | sed 's/pod\///' | grep -v "$PRIMARY_POD" | head -n 1)
    
    log_msg "[$(date '+%H:%M:%S')] 🎯 Primary: $PRIMARY_POD | Standby: $STANDBY_POD"
    
    START_TIME=$(date +%s)
    log_msg "[$(date '+%H:%M:%S')] 💥 Executing primary deletion on $PRIMARY_POD..."
    kubectl delete pod $PRIMARY_POD --now -n default
    
    log_msg "[$(date '+%H:%M:%S')] 👑 Polling standby pod ($STANDBY_POD) for Leadership Acquired timestamp..."
    kubectl exec $STANDBY_POD -n default -- sh -c 'tail -n 0 -F /tmp/ray/session_latest/logs/gcs_server.out | grep -m 1 "Acquired leadership from Lease, promoting GCS."'
    LEADER_TIME=$(date +%s)
    LEADER_ELAPSED=$((LEADER_TIME - START_TIME))
    log_msg "[$(date '+%H:%M:%S')] 👑 ✅ Leadership successfully acquired by $STANDBY_POD! (Elapsed: ${LEADER_ELAPSED}s)"
    
    log_msg "[$(date '+%H:%M:%S')] 🟢 Polling API until $STANDBY_POD transitions to Ready=True..."
    kubectl wait --for=condition=Ready pod/$STANDBY_POD -n default --timeout=60s
    READY_TIME=$(date +%s)
    READY_ELAPSED=$((READY_TIME - START_TIME))
    log_msg "[$(date '+%H:%M:%S')] 🟢 ✅ Pod $STANDBY_POD successfully passed Readiness Probe! (Elapsed: ${READY_ELAPSED}s)"
    
    log_msg "[$(date '+%H:%M:%S')] 📡 Polling Raylet logs retroactively on $ACTIVE_WORKER to verify resubscription event without race conditions..."
    while true; do
        if kubectl exec $ACTIVE_WORKER -n default -- grep -q "Finished fetching all node address and liveness information for resubscription" /tmp/ray/session_latest/logs/raylet.out; then
            break
        fi
        sleep 2
    done
    
    END_TIME=$(date +%s)
    RECOVERY_TIME=$((END_TIME - START_TIME))
    
    log_msg "[$(date '+%H:%M:%S')] 📡 Reconnection captured! (Total recovery duration: ${RECOVERY_TIME}s)"

    log_msg "[$(date '+%H:%M:%S')] 🔄 Refreshing local port-forward tunnel to seamlessly bind to the newly promoted HA service endpoint..."
    if [ -n "$PF_PID" ]; then
        kill $PF_PID >/dev/null 2>&1 || true
    fi
    kubectl port-forward svc/ray-self-contained-gcs-head-svc 8265:8265 -n default > /dev/null 2>&1 &
    PF_PID=$!
    sleep 10
    
    log_msg "[$(date '+%H:%M:%S')] 🔍 Verifying persistent GCS data recovery: Ensuring prior pre-failover Job ID ($PRE_FAILOVER_JOB_ID) is perfectly preserved and tracked by the newly promoted primary head..."
    
    LIST_RETRIES=6
    LIST_SUCCESS=false
    
    for i in $(seq 1 $LIST_RETRIES); do
        log_msg "[$(date '+%H:%M:%S')] 🔍 Attempting GCS execution history retrieval (Attempt $i/$LIST_RETRIES)..."
        if JOB_LIST_OUT=$(RAY_ADDRESS="http://localhost:8265" ray job list 2>&1); then
            if echo "$JOB_LIST_OUT" | grep -q "$PRE_FAILOVER_JOB_ID"; then
                LIST_SUCCESS=true
                break
            fi
        fi
        sleep 5
    done

    if [ "$LIST_SUCCESS" == true ]; then
        log_msg "[$(date '+%H:%M:%S')] 🔍 ✅ SUCCESS: Pre-failover execution history perfectly recovered and listed directly by the newly promoted active head!"
    else
        log_msg "[$(date '+%H:%M:%S')] 🚨 CRITICAL: Failed to locate persistent pre-failover execution Job ID in the active history!"
        exit 1
    fi

    log_msg "[$(date '+%H:%M:%S')] ⚙️ Submitting completely new verification workload directly over HA forwarded dashboard tunnel to conclusively prove full functionality on promoted head..."
    
    SUBMIT_RETRIES=6
    SUBMIT_SUCCESS=false
    
    for i in $(seq 1 $SUBMIT_RETRIES); do
        log_msg "[$(date '+%H:%M:%S')] 🚀 Executing post-failover job payload submission (Attempt $i/$SUBMIT_RETRIES)..."
        if RAY_ADDRESS="http://localhost:8265" ray job submit --working-dir /tmp/ray_job_demo -- python script.py; then
            SUBMIT_SUCCESS=true
            break
        fi
        sleep 5
    done
    
    if [ "$SUBMIT_SUCCESS" == true ]; then
        log_msg "[$(date '+%H:%M:%S')] ✅ New post-failover benchmark computation successfully finalized via Ray Dashboard API!"
    else
        log_msg "[$(date '+%H:%M:%S')] 🚨 CRITICAL: Post-failover computation submission completely failed!"
        exit 1
    fi
    
    CURRENT_RESTARTS=$(kubectl get pod $ACTIVE_WORKER -n default -o jsonpath='{.status.containerStatuses[0].restartCount}')
    if [ "$CURRENT_RESTARTS" -gt "$INITIAL_RESTARTS" ]; then
        log_msg "[$(date '+%H:%M:%S')] 🚨 CRITICAL: Worker container restart detected! Stopping infinite loop immediately!"
        exit 1
    fi
    
    log_msg "[$(date '+%H:%M:%S')] ⏳ Waiting for replacement secondary head pod to fully stabilize..."
    while true; do
        SHADOW_COUNT=$(kubectl get pods -l ray.io/node-type=head -n default --field-selector=status.phase=Running | grep -c Running || true)
        if [ "$SHADOW_COUNT" -ge 2 ]; then
            break
        fi
        sleep 5
    done
    
    log_msg "[$(date '+%H:%M:%S')] 🔒 Full cluster stabilized. Waiting 10s..."
    sleep 10
    
    ((ITERATION++))
done
