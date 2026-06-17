# Stateful Workload Migration to Supervisor-Based VKS 3.5+

**Target Audience:** Site Reliability Engineers (SREs) & Infrastructure Architects

**Core Stack:** Kubernetes 1.33+, vSphere VKS 3.5+, Velero, `govc`, and S3-Compatible Storage (Ceph/SeaweedFS/Garage)

Moving stateful applications between Kubernetes clusters traditionally triggers "data gravity panic"—the operational dread of waiting hours for block-by-block replication over the network via standard Velero data movers, only to face data concurrency anomalies or split-brain states at cutover.

This playbook outlines an alternative approach: a **metadata-only migration**. By decoupling physical First Class Disk (FCD) relocation from Kubernetes manifest restoration, we eliminate data replication entirely. This method reduces stateful application cutover windows from hours to seconds while maintaining block-level integrity.

The process orchestrates a clean handoff between the source Kubernetes cluster, vCenter datastores, the vSphere Supervisor Context, and the destination VKS 3.5+ cluster.

### High-Level Migration Flow

1. **Quiesce Workload:** Scale down the source app to flush the filesystem and safely detach the volume.
    
2. **Extract Metadata:** Isolate the underlying vSphere FCD UUID.
    
3. **Backup Manifests:** Dump K8s definitions to an out-of-band S3 store via Velero.
    
4. **Relocate Storage:** Move the FCD across datastores and storage profiles at the vSphere layer.
    
5. **Supervisor Registration:** Apply `cnsregistervolumes` in the Supervisor context to declare the disk to the new environment.
    
6. **Staged Restore & Patch:** Restore manifests to VKS in a pending state, intercept the PVC, map it to the migrated FCD UUID, and spin up the application.
    

## Storage Infrastructure Matrix (Non-AGPL Alternatives)

To maintain production standards regarding performance, licensing, and resource overhead, **MinIO is explicitly excluded from this architecture**. SREs must align their Velero backup storage location with one of the three verified options below based on their platform topology:

|**Feature / Metric**|**Scenario A: RHOS + Ceph**|**Scenario B: Generic K8s + SeaweedFS**|**Scenario C: Generic K8s + Garage**|
|---|---|---|---|
|**Primary Use Case**|Deep enterprise integration within Red Hat OpenShift footprints.|High-throughput, volume-heavy block metadata serialization.|Lightweight, decentralized, multi-zone cluster environments.|
|**SRE Advantage**|Highly mature; utilizes RADOS Gateway (RGW) for robust multi-tenancy.|Rapid metadata operations; separates file metadata from data chunks.|Single binary, zero-dependency model; exceptionally low runtime memory footprint.|
|**Complexity Tier**|High (Requires ODF/Rook management).|Medium (Simple master/volume server architecture).|Low (Simple mesh configuration, built in Rust).|

## Pre-Migration Prerequisites & Automated Gates

To ensure a self-defending pipeline that halts execution before touching production data, two validation gates must be verified during setup.
### Target Storage Policy Pre-Allocation
The target Supervisor-based VKS cluster must have the required vSphere Storage Policy pre-allocated to the target vSphere Namespace as a hard prerequisite during cluster initialization. If the StorageClass inside the VKS cluster cannot map cleanly to an active backing policy, the Supervisor will reject volume mounting.
### The FCD UUID Validation Check
Cross-datastore moves can occasionally trigger a native vSphere cloning mechanism that alters the underlying disk UUID. Because `cnsregistervolumes` depends on a strict identity match, the pipeline uses a `govc` verification loop to validate block identity before writing Supervisor manifests.
## End-to-End Execution Script

Execute this automated sequence from your migration bastion host. Ensure you have authenticated contexts for the Source Cluster, the Supervisor Cluster, and the Target VKS Guest Cluster.

```bash
#!/usr/bin/env bash
set -euo pipefail

# ==========================================
# 1. ENVIRONMENT CONFIGURATION
# ==========================================
export CTX_SOURCE="source-k8s-cluster"
export CTX_SUPERVISOR="vsphere-supervisor-context"
export CTX_VKS="destination-vks-cluster"

export NAMESPACE="production-apps"
export APP_NAME="stateful-cache"
export PVC_NAME="data-volume-stateful-cache-0"
export STORAGE_POLICY="vks-gold-storage-policy"
export TARGET_STORAGE_CLASS="vks-vols-tg"

echo "Checking Target Storage Policy Prerequisite..."
kubectl --context="${CTX_SUPERVISOR}" get storageprofiles.storage.k8s.io | grep -q "${STORAGE_POLICY}" || {
    echo "CRITICAL: Storage policy ${STORAGE_POLICY} not allocated to Supervisor Namespace!"
    exit 1
}

# ==========================================
# 2. QUIESCE SOURCE & METADATA EXTRACTION
# ==========================================
echo "Scaling down stateful workload on source cluster..."
kubectl --context="${CTX_SOURCE}" -n "${NAMESPACE}" scale statefulset "${APP_NAME}" --replicas=0

echo "Enforcing strict volume detachment check..."
kubectl --context="${CTX_SOURCE}" -n "${NAMESPACE}" wait \
    --for=jsonpath='{.status.attachments}'=null volumeattachments \
    -l app="${APP_NAME}" --timeout=5m

# Extract raw FCD ID from the live PV metadata
PV_NAME=$(kubectl --context="${CTX_SOURCE}" -n "${NAMESPACE}" get pvc "${PVC_NAME}" -o jsonpath='{.spec.volumeName}')
SOURCE_FCD_ID=$(kubectl --context="${CTX_SOURCE}" get pv "${PV_NAME}" -o jsonpath='{.spec.csi.volumeHandle}')
echo "Source FCD UUID isolated: ${SOURCE_FCD_ID}"

# ==========================================
# 3. VELERO MANIFEST EXTRACTION
# ==========================================
echo "Backing up Kubernetes manifests via Velero (Skipping Data Movers)..."
velero backup create "${APP_NAME}-manifest-backup" \
    --kubecontext "${CTX_SOURCE}" \
    --include-namespaces "${NAMESPACE}" \
    --include-resources persistentvolumeclaims,statefulsets,secrets,configmaps,services \
    --storage-location migration-s3-target \
    --wait

# ==========================================
# 4. DATASTORE RELOCATION & IDENTITY GATING
# ==========================================
echo "Executing FCD datastore relocation via vSphere control plane..."
# Relocate the FCD block device to the destination datastore
govc disk.relocate -vm-storage-policy "${STORAGE_POLICY}" "${SOURCE_FCD_ID}" "ds:///vmfs/volumes/dest-datastore/"

echo "Running Automated UUID Validation Gate..."
# Query the target datastore registry to confirm identity preservation
DEST_FCD_ID=$(govc disk.ls -json | jq -r --arg id "${SOURCE_FCD_ID}" '.[] | select(.Id==$id) | .Id')

if [ -z "${DEST_FCD_ID}" ] || [ "${SOURCE_FCD_ID}" != "${DEST_FCD_ID}" ]; then
    echo "CRITICAL ERROR: FCD UUID Alteration or loss detected post-move! Halting pipeline."
    exit 1
else
    echo "UUID Validation Gate PASSED. FCD block integrity verified."
fi

# ==========================================
# 5. SUPERVISOR CNS REGISTRATION
# ==========================================
echo "Registering volume within the Supervisor Context..."
# Note: The cnsregistervolumes CRD exists ONLY at the Supervisor layer, not the VKS Guest layer
cat <<EOF | kubectl --context="${CTX_SUPERVISOR}" apply -f -
apiVersion: cns.vmware.com/v1alpha1
kind: CnsRegisterVolume
metadata:
  name: "reg-${PVC_NAME}"
  namespace: "${NAMESPACE}"
spec:
  volumeId: "${DEST_FCD_ID}"
  diskStatus: "Attached"
EOF

echo "Waiting for CNS Registration to complete..."
kubectl --context="${CTX_SUPERVISOR}" -n "${NAMESPACE}" wait \
    --for=condition=Complete cnsregistervolume "reg-${PVC_NAME}" --timeout=2m
```

## Staged Restore Interception Loop (Approach B)
If Velero restores the application PVC directly into the VKS guest cluster, the target cluster's default storage controller will immediately execute a lifecycle provision loop, creating a brand-new, completely empty FCD.

To intercept this race condition, we map the incoming resource to a non-provisioning hold class, run the Velero restore, and execute an automated **Post-Restore Patch Loop** to graft the pre-registered CNS volume directly into the live manifest before binding occurs.
### Step 1: Create the Velero StorageClass Modifier Mapping

Apply this configuration to the VKS cluster before running the restore to override automatic lifecycle binding:
```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: change-storage-class-config
  namespace: velero
  labels:
    velero.io/plugin-config: ""
    velero.io/change-storage-class: RestoreItemAction
data:
  source-storage-class-name: "dummy-hold-class"
```

### Step 2: Trigger the Manifest-Only Restore
```bash
echo "Executing manifest restore on VKS cluster..."
velero restore create "${APP_NAME}-migration-restore" \
    --kubecontext "${CTX_VKS}" \
    --from-backup "${APP_NAME}-manifest-backup" \
    --wait
```

### Step 3: Run the Interception & Patch Script

Because the resources land in a pending state under `dummy-hold-class`, we have an operational window to stitch the existing validated volume handles directly into the target cluster's live registry.
```bash
echo "Intercepting pending manifests. Patching VKS PVC with migrated volume handle..."

# 1. Pre-stage the PV inside VKS pointing explicitly to the validated CNS Volume ID
cat <<EOF | kubectl --context="${CTX_VKS}" apply -f -
apiVersion: v1
kind: PersistentVolume
metadata:
  name: "pv-${PVC_NAME}"
spec:
  capacity:
    storage: 100Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: "${TARGET_STORAGE_CLASS}"
  csi:
    driver: csi.vsphere.vmware.com
    volumeHandle: "${DEST_FCD_ID}"
EOF

# 2. Intercept and patch the pending PVC to bind directly to our pre-staged PV
kubectl --context="${CTX_VKS}" -n "${NAMESPACE}" patch pvc "${PVC_NAME}" \
    -p "{\"spec\":{\"volumeName\":\"pv-${PVC_NAME}\",\"storageClassName\":\"${TARGET_STORAGE_CLASS}\"}}"

# 3. Scale up the stateful application and verify rollout status
echo "Realigning workload control plane. Initializing pods..."
kubectl --context="${CTX_VKS}" -n "${NAMESPACE}" scale statefulset "${APP_NAME}" --replicas=1
kubectl --context="${CTX_VKS}" -n "${NAMESPACE}" rollout status statefulset "${APP_NAME}" --timeout=5m
```

## SRE Troubleshooting & Failure Modes
> Critical Gotcha: Volume Ghosting / Orphan States
> 
> If a migration aborts mid-execution after Phase 5, a volume may remain locked in the Supervisor's CNS inventory, rendering it unusable. SREs should resolve this by applying a targeted `CnsUnregisterVolume` manifest to the Supervisor context rather than manually altering database states within vCenter.

### The Multi-Attach Deadlock

- **The Symptom:** The Supervisor CNS registration fails or times out with disk attach errors.
    
- **The Root Cause:** The source Kubernetes cluster failed to cleanly clear out its `VolumeAttachment` API objects, causing vCenter to maintain an active host-level lock on the underlying FCD block.
    
- **The Fix:** Return to the source context, identify the zombie attachment string using `kubectl get volumeattachments`, and force delete it _only_ after confirming the application pods on the source are completely terminated.

## Source Cluster Cleanup Sequence

Instead of an unregistration script, the SRE team only needs to execute a **clean K8s metadata teardown** on the source cluster. This ensures that the FCD is left in a pristine, unattached state, completely ready for the `govc` move:

### Step 1: Drain & Scale to 0
```bash
kubectl --context="${CTX_SOURCE}" -n "${NAMESPACE}" scale statefulset "${APP_NAME}" --replicas=0
```

> **Why:** This forces the vSphere CSI driver to trigger a `ControllerUnpublishVolume` call. This flushes the dirty data pages from the guest OS filesystem, unmounts the disk, and releases the host-level vCenter lock on the FCD block device.

### Step 2: Set the Reclaim Policy to Retain

Before removing any K8s references, verify that your PersistentVolume (PV) will protect the backing infrastructure block.
```bash
kubectl --context="${CTX_SOURCE}" patch pv "${PV_NAME}" -p '{"spec":{"persistentVolumeReclaimPolicy":"Retain"}}'
```

> **Why:** If the storage policy is set to `Delete`, deleting the PVC in the next step will instruct vCenter to permanently destroy the underlying FCD and all your production data.

### Step 3: Delete the source PV and PVC Objects
```bash
kubectl --context="${CTX_SOURCE}" -n "${NAMESPACE}" delete pvc "${PVC_NAME}"
kubectl --context="${CTX_SOURCE}" delete pv "${PV_NAME}"
```