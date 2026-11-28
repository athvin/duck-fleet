{{- define "duckfleet.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
readOnlyRootFilesystem: true
{{- end -}}
{{/*
Where every pod in this release runs, emitted from one place.

Both halves are load-bearing and only work together: the selector sends DuckFleet pods
to the node pool DuckFleet owns, and the toleration is what lets them land on it, since
that pool is tainted so nothing else can. Emitting them separately is how a pod ends up
with one and not the other — selector without toleration never schedules, toleration
without selector consumes a neighbour's capacity. scripts/`duckfleet-ops check-chart` asserts the
session pod template matches the persistent workers, so a caller asking for isolation
cannot be placed somewhere the shared tier would not have been.
*/}}
{{- define "duckfleet.placement" -}}
{{- /*
Null-valued nodeSelector keys are dropped, not emitted. A deploy overlay removes the
chart's default compute-type selector by setting it null (Helm's documented way to
delete a default key on merge), but whether that null is erased or kept depends on the
Helm version and the render path — and a null that survives becomes `key: null` in the
pod, an impossible label match that leaves the pod unschedulable and Karpenter unable
to provision for it (the isolated session pod's exact failure, #90). Filtering here
makes the outcome deterministic regardless of how the merge resolved the null.
*/ -}}
{{- $selector := dict -}}
{{- range $key, $value := .Values.nodeSelector -}}
{{- if not (kindIs "invalid" $value) -}}{{- $_ := set $selector $key $value -}}{{- end -}}
{{- end -}}
nodeSelector: {{ toJson $selector }}
{{- if .Values.tolerations }}
tolerations: {{ toJson .Values.tolerations }}
{{- end }}
{{- end -}}
{{- define "duckfleet.podSecurity" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{/* Parse the binary quantities the worker accepts; values.schema.json bounds
     spelling and length so Helm arithmetic cannot overflow. */}}
{{- define "duckfleet.quantityBytes" -}}
{{- $number := regexFind "^[0-9]+" . | int64 -}}
{{- if hasSuffix "Gi" . -}}{{ mul $number 1073741824 }}
{{- else if hasSuffix "Mi" . -}}{{ mul $number 1048576 }}
{{- else -}}{{ mul $number 1024 }}{{- end -}}
{{- end -}}
{{- define "duckfleet.diskLimit" -}}
{{- $bytes := add (include "duckfleet.quantityBytes" . | int64) 1073741824 -}}
{{- if eq (mod $bytes 1073741824) 0 -}}{{ div $bytes 1073741824 }}Gi
{{- else if eq (mod $bytes 1048576) 0 -}}{{ div $bytes 1048576 }}Mi
{{- else -}}{{ div $bytes 1024 }}Ki{{- end -}}
{{- end -}}
{{/* Stable recovery identities. Only small uses the historical spelling;
     other pool names can never collide with small-1 or another pool's index. */}}
{{- define "duckfleet.workerId" -}}
{{- if eq .pool "small" -}}
{{- ternary "small" (printf "small-%d" (int .index)) (eq (int .index) 0) -}}
{{- else -}}{{ printf "pool-%s-%d" .pool (int .index) }}{{- end -}}
{{- end -}}
{{- define "duckfleet.initialWorkerReplicas" -}}
{{- if .root.Values.suspended -}}0
{{- else -}}
{{- $policy := get .root.Values.autoscaling .pool -}}
{{- $minimum := 1 -}}
{{- if $policy -}}{{- $minimum = get $policy "min_workers" | default 1 -}}{{- end -}}
{{- ternary 0 1 (and (not (empty $policy)) (ge (int .index) (int $minimum))) -}}
{{- end -}}
{{- end -}}
{{/* One validated pool map generates workers and gateway endpoints together. */}}
{{- define "duckfleet.pools" -}}
{{- $pools := dict -}}
{{- if .Values.persistentPools -}}
{{- if .Values.workerReplicas -}}
{{- fail "persistentPools cannot be combined with workerReplicas; clear legacy workerReplicas when adopting named pools" -}}
{{- end -}}
{{- range $name, $definition := .Values.persistentPools -}}
{{- $pool := deepCopy $definition -}}
{{- $authority := get $pool "authority" | default "writer" -}}
{{- if not (has $authority (list "reader" "writer")) -}}{{- fail (printf "pool %s authority must be reader or writer" $name) -}}{{- end -}}
{{- $_ := set $pool "authority" $authority -}}
{{- if not (hasKey $pool "workers") -}}{{- $_ := set $pool "workers" 1 -}}{{- end -}}
{{- if not (hasKey $pool "maxSessionsPerWorker") -}}{{- $_ := set $pool "maxSessionsPerWorker" $.Values.workerMaxSessions -}}{{- end -}}
{{- if lt (include "duckfleet.quantityBytes" $pool.memory | int64) 268435456 -}}
{{- fail (printf "pool %s memory must be at least 256Mi" $name) -}}
{{- end -}}
{{- if lt (include "duckfleet.quantityBytes" $pool.disk | int64) 1073741824 -}}
{{- fail (printf "pool %s disk must be at least 1Gi" $name) -}}
{{- end -}}
{{- with $pool.engine -}}
{{- if and (hasKey . "threads") (gt (int .threads) (int $pool.cpu)) -}}
{{- fail (printf "pool %s engine.threads must not exceed its cpu" $name) -}}
{{- end -}}
{{- if and (hasKey . "memory") (gt (include "duckfleet.quantityBytes" .memory | int64) (div (mul (include "duckfleet.quantityBytes" $pool.memory | int64) 90) 100)) -}}
{{- fail (printf "pool %s engine.memory must be at most 90%% of its memory" $name) -}}
{{- end -}}
{{- end -}}
{{- $_ := set $pools $name $pool -}}
{{- end -}}
{{- else -}}
{{- $_ := set $pools "small" (dict "cpu" 1 "memory" "1Gi" "disk" "2Gi" "workers" (dig "small" 1 (.Values.workerReplicas | default dict)) "maxSessionsPerWorker" .Values.workerMaxSessions "authority" "writer") -}}
{{- end -}}
{{- if not (hasKey $pools .Values.defaultPool) -}}{{- fail "defaultPool must name a configured persistent pool" -}}{{- end -}}
{{- $identities := dict -}}
{{- range $name, $pool := $pools -}}
{{- range $index := until (int $pool.workers) -}}
{{- $id := include "duckfleet.workerId" (dict "pool" $name "index" $index) -}}
{{- if or (hasKey $identities $id) (gt (len (printf "worker-%s" $id)) 63) -}}
{{- fail (printf "generated worker identity collides or is too long: %s" $id) -}}
{{- end -}}
{{- $_ := set $identities $id true -}}
{{- end -}}
{{- end -}}
{{- toJson $pools -}}
{{- end -}}
{{- define "duckfleet.poolEndpoints" -}}
{{- $urls := list -}}
{{- range $index := until (int .workers) -}}
{{- $id := include "duckfleet.workerId" (dict "pool" $.name "index" $index) -}}
{{- $urls = append $urls (printf "http://worker-%s:8080" $id) -}}
{{- end -}}
{{- toJson $urls -}}
{{- end -}}
{{- define "duckfleet.workerUrls" -}}
{{- $urls := list -}}
{{- range $name, $pool := include "duckfleet.pools" . | fromJson -}}
{{- $urls = concat $urls (include "duckfleet.poolEndpoints" (dict "name" $name "workers" $pool.workers) | fromJsonArray) -}}
{{- end -}}
{{- join "," $urls -}}
{{- end -}}
{{- define "duckfleet.poolConfig" -}}
{{- $pools := dict -}}
{{- range $name, $pool := include "duckfleet.pools" . | fromJson -}}
{{- $configured := dict "cpu" $pool.cpu "memory" $pool.memory "disk" $pool.disk "authority" $pool.authority "endpoints" (include "duckfleet.poolEndpoints" (dict "name" $name "workers" $pool.workers) | fromJsonArray) "max_sessions_per_worker" $pool.maxSessionsPerWorker -}}
{{- if hasKey $pool "jobLimits" -}}
{{- include "duckfleet.validateJobLimits" (dict "limits" $pool.jobLimits "ceiling" ($.Values.jobPolicy.ceilings | default dict)) -}}
{{- $_ := set $configured "job_limits" $pool.jobLimits -}}
{{- end -}}
{{- if hasKey $pool "admission" -}}
{{- $_ := set $configured "admission" (include "duckfleet.admissionJson" $pool.admission | fromJson) -}}
{{- end -}}
{{- include "duckfleet.validateAdmission" (mergeOverwrite (deepCopy ($.Values.admissionLimits.default | default dict)) ($pool.admission | default dict)) -}}
{{- $_ := set $pools $name $configured -}}
{{- end -}}
{{- toJson (dict "default_pool" .Values.defaultPool "pools" $pools) -}}
{{- end -}}
{{/* Preserve omitted fields until global and pool overrides have been merged. */}}
{{- define "duckfleet.admissionJson" -}}
{{- $json := dict -}}
{{- range $camel, $snake := dict "maxQueuedQueries" "max_queued_queries" "maxRunningQueries" "max_running_queries" "maxQueuedPerPrincipal" "max_queued_per_principal" "maxRunningPerPrincipal" "max_running_per_principal" -}}
{{- if hasKey $ $camel -}}{{- $_ := set $json $snake (get $ $camel | int64) -}}{{- end -}}
{{- end -}}
{{- toJson $json -}}
{{- end -}}
{{- define "duckfleet.validateAdmission" -}}
{{- $queued := int64 (get . "maxQueuedQueries" | default 256) -}}
{{- $running := int64 (get . "maxRunningQueries" | default 16) -}}
{{- $principalQueued := int64 (get . "maxQueuedPerPrincipal" | default 16) -}}
{{- $principalRunning := int64 (get . "maxRunningPerPrincipal" | default $running) -}}
{{- if or (gt $principalQueued $queued) (gt $principalRunning $running) -}}
{{- fail "admission principal limits cannot exceed effective pool limits" -}}
{{- end -}}
{{- end -}}
{{- define "duckfleet.admissionConfig" -}}
{{- $config := dict -}}
{{- range $scope, $limits := .Values.admissionLimits -}}
{{- include "duckfleet.validateAdmission" $limits -}}
{{- $_ := set $config $scope (include "duckfleet.admissionJson" $limits | fromJson) -}}
{{- end -}}
{{- toJson $config -}}
{{- end -}}
{{/* Runtime exact-version checks remain authoritative; this marks rollout intent. */}}
{{- define "duckfleet.platformSchema" -}}9{{- end -}}
{{/* Supplied envelopes are complete (JSON schema); each pool remains inside the
operator envelope. Defaults may exceed it because omitted request options clip. */}}
{{- define "duckfleet.validateJobLimits" -}}
{{- $hard := dict "runtime_seconds" 3600 "result_bytes" 1073741824 "result_rows" 10000000 "retention_seconds" 86400 -}}
{{- range $key, $value := .limits -}}
{{- $maximum := get $.ceiling $key | default (get $hard $key) | int64 -}}
{{- if gt (int64 $value) $maximum -}}{{- fail (printf "job limit %s exceeds operator ceiling" $key) -}}{{- end -}}
{{- end -}}
{{- end -}}
{{- define "duckfleet.jobPolicy" -}}
{{- if hasKey .Values.jobPolicy "isolated" -}}
{{- include "duckfleet.validateJobLimits" (dict "limits" .Values.jobPolicy.isolated "ceiling" (.Values.jobPolicy.ceilings | default dict)) -}}
{{- end -}}
{{- toJson .Values.jobPolicy -}}
{{- end -}}
{{/*
The worker container's environment, defined once. The persistent Deployments and the
isolated pod template both render from this, so they cannot drift apart on secret
names, S3 configuration, or the resource declaration the worker checks its own cgroup
against. Takes a dict of `root`, `id`, `cpu` and `memory`.

An isolated pod's identity is only known when it is created, so the gateway overwrites
`DUCKFLEET_WORKER_ID` and the two resource declarations in the template it is handed.
It refuses to start a pod if any of the three is missing, which is what keeps this
template and the code that patches it from drifting silently.
*/}}
{{/* Results never share DuckLake's managed prefix. Only paths, not lake
credentials, are sent to the gateway for the same runtime overlap validation. */}}
{{- define "duckfleet.resultEnv" -}}
{{- $results := .Values.results | default dict -}}
{{- if $results.enabled -}}
{{- $lake := trimSuffix "/" .Values.lakePrefix -}}
{{- $result := trimSuffix "/" $results.prefix -}}
{{- if or (eq $lake $result) (hasPrefix (printf "%s/" $lake) $result) (hasPrefix (printf "%s/" $result) $lake) -}}
{{- fail "results.prefix and lakePrefix must be separate non-overlapping prefixes" -}}
{{- end -}}
- {name: DUCKFLEET_RESULT_DATA_PATH, value: {{ printf "s3://%s/%s/" .Values.bucket $result | quote }}}
- {name: DUCKFLEET_RESULT_LAKE_DATA_PATH, value: {{ printf "s3://%s/%s/" .Values.bucket $lake | quote }}}
- {name: DUCKFLEET_RESULT_AUTH_MODE, value: pod_identity}
- {name: DUCKFLEET_RESULT_ALLOW_HTTP, value: "false"}
{{- end -}}
{{- end -}}

{{- define "duckfleet.workerEnv" -}}
{{- $ := .root -}}
{{- $authority := .authority | default "writer" -}}
{{- if gt (add $.Values.catalogWaitSeconds $.Values.platformWaitSeconds) 600 -}}
{{- fail "catalogWaitSeconds + platformWaitSeconds must fit the 600-second startup probe budget" -}}
{{- end -}}
- {name: DUCKFLEET_WORKER_ID, value: {{ .id | quote }}}
- {name: DUCKFLEET_AUTHORITY, value: {{ $authority | quote }}}
{{- if hasKey . "pool" }}
- {name: DUCKFLEET_POOL, value: {{ .pool | quote }}}
{{- end }}
- {name: DUCKFLEET_WORKER_MAX_SESSIONS, value: {{ .maxSessions | default 1 | quote }}}
- {name: DUCKFLEET_WORKER_QUEUE_CAPACITY, value: {{ $.Values.workerQueueCapacity | quote }}}
- {name: DUCKFLEET_WORKER_MAX_QUEUED_QUERIES, value: {{ $.Values.workerMaxQueuedQueries | int64 | quote }}}
- {name: DUCKFLEET_WORKER_MAX_QUEUED_BYTES, value: {{ $.Values.workerMaxQueuedBytes | int64 | quote }}}
# Declared from the same values as the limits. The worker refuses to start if the
# container it actually got disagrees.
- {name: DUCKFLEET_WORKER_CPU, value: {{ .cpu | quote }}}
- {name: DUCKFLEET_WORKER_MEMORY, value: {{ .memory | quote }}}
# Disk is declared-only (no cgroup to check against), so the worker trusts this to
# match the ephemeral-storage request/limit above. The gateway overwrites it, with
# cpu and memory, in the isolated pod template before creating a sized session.
- {name: DUCKFLEET_WORKER_DISK, value: {{ .disk | quote }}}
# DuckDB's memory_limit and threads. Empty means automatic: an isolated pod sizes the
# engine from the request (half the memory, a thread per CPU); a persistent worker
# defaults to 40% of its memory, since every session on it shares one engine. A
# persistent pool sets these with `engine`; isolated pods only through
# isolatedCompute.engineOverride. See docs/decisions/0002-engine-limits.md.
{{- with .engine }}
{{- if hasKey . "memory" }}
- {name: DUCKFLEET_ENGINE_MEMORY, value: {{ .memory | quote }}}
{{- end }}
{{- if hasKey . "memoryPercent" }}
- {name: DUCKFLEET_ENGINE_MEMORY_PERCENT, value: {{ .memoryPercent | quote }}}
{{- end }}
{{- if hasKey . "threads" }}
- {name: DUCKFLEET_ENGINE_THREADS, value: {{ .threads | quote }}}
{{- end }}
{{- end }}
- {name: S3_AUTH_MODE, value: pod_identity}
{{- if eq $authority "writer" }}
{{ include "duckfleet.resultEnv" $ }}
{{- end }}
- {name: DUCKFLEET_PROFILING, value: {{ $.Values.profiling | quote }}}
- {name: DUCKFLEET_PROFILE_SAMPLE_PERCENT, value: {{ $.Values.profileSamplePercent | quote }}}
- {name: DUCKFLEET_COMPUTE_POLICY_JSON, value: {{ $.Values.computePolicy | toJson | quote }}}
- {name: DUCKFLEET_CATALOG_WAIT_SECONDS, value: {{ $.Values.catalogWaitSeconds | quote }}}
- {name: DUCKFLEET_PLATFORM_WAIT_SECONDS, value: {{ $.Values.platformWaitSeconds | quote }}}
# The rate card rides with every role that prices work: workers admit and settle
# with it, and each reservation copies it so recovery never reprices old work.
# `duckfleet-ops check-chart` asserts the gateway carries the same pair.
- {name: DUCKFLEET_RATE_CARD_VERSION, value: {{ $.Values.rateCard.version | quote }}}
- {name: DUCKFLEET_COMPUTE_MICRO_USD_PER_UNIT_HOUR, value: {{ $.Values.rateCard.computeMicroUsdPerUnitHour | quote }}}
- {name: S3_DATA_PATH, value: {{ printf "s3://%s/%s/" $.Values.bucket $.Values.lakePrefix | quote }}}
- {name: AWS_REGION, value: {{ $.Values.region }}}
- {name: AWS_EC2_METADATA_DISABLED, value: "true"}
- {name: DUCKFLEET_NAMESPACE, value: {{ $.Release.Namespace | quote }}}
- name: DUCKFLEET_INTERNAL_TOKEN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: DUCKFLEET_INTERNAL_TOKEN}
{{- if eq $authority "reader" }}
- name: CATALOG_READER_DSN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: CATALOG_READER_DSN}
{{- else }}
- name: CATALOG_DSN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: CATALOG_DSN}
{{- end }}
- name: PLATFORM_DSN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: PLATFORM_WORKER_DSN}
{{- end -}}
{{/*
The pod an isolated session runs in. The gateway replaces its name, its owner
reference, its resources and the three declarations above before creating it.

Not a Deployment or a Job: both would recreate the pod after it exits, attaching the
catalog again under an identity whose session the gateway has already forgotten.
*/}}
{{- define "duckfleet.isolatedPod" -}}
apiVersion: v1
kind: Pod
metadata:
  annotations:
    duckfleet.io/platform-schema: {{ include "duckfleet.platformSchema" . | quote }}
  labels:
    app: duckfleet-worker
    duckfleet-compute: isolated
spec:
  {{- include "duckfleet.termination" . | nindent 2 }}
  # One session, one run. A crashed engine must leave a pod to be collected, not a
  # restarted container the gateway no longer has a route to.
  restartPolicy: Never
  # Bounds the pod's life even if both the gateway and the owner reference fail to
  # remove it. A backstop, not the mechanism.
  activeDeadlineSeconds: {{ .Values.isolatedCompute.maxSessionSeconds }}
  # The S3 identity the persistent workers use, with no API token: an isolated pod
  # runs user SQL and must not inherit the gateway's ability to create pods.
  serviceAccountName: duckfleet-worker
  automountServiceAccountToken: false
  {{- include "duckfleet.placement" . | nindent 2 }}
  securityContext:
    {{- include "duckfleet.podSecurity" . | nindent 4 }}
  containers:
    - name: worker
      image: {{ required "workerImage must be an immutable ECR digest" .Values.workerImage | quote }}
      securityContext:
        {{- include "duckfleet.containerSecurity" . | nindent 8 }}
      # Replaced per session. CPU and memory requests equal their limits, unlike the
      # persistent workers: a dedicated pod has no neighbour to borrow headroom from,
      # and the caller asked for a size rather than a share.
      resources:
        requests: {cpu: "1", memory: 1Gi, ephemeral-storage: 2Gi}
        limits: {cpu: "1", memory: 1Gi, ephemeral-storage: 3Gi}
      env:
        {{- include "duckfleet.workerEnv" (dict "root" . "id" "replaced-per-session" "cpu" "1" "memory" "1Gi" "disk" "2Gi" "engine" (include "duckfleet.isolatedEngine" . | fromJson)) | nindent 8 }}
        # Declares that this process is a pod created for one session. The worker
        # admits exactly one isolated session and refuses shared ones, and reports
        # the mode to history from this rather than from the request, so a caller
        # cannot relabel where their statement ran.
        - {name: DUCKFLEET_ISOLATED, value: "true"}
      {{- include "duckfleet.health" . | nindent 6 }}
      volumeMounts:
        - {name: tmp, mountPath: /tmp}
  volumes:
    - name: tmp
      emptyDir: {sizeLimit: 2Gi}
{{- end -}}
{{/*
How long the kubelet waits after SIGTERM before killing a worker.

It must exceed the longest statement a caller may ask for, which the API caps at 600
seconds. Unset it defaults to 30, so a query in its second half could never finish
however well the drain was written — the two numbers are coupled, and `duckfleet-ops check-chart`
asserts it so a later change to either fails rather than silently truncating drains.
The margin covers the engine rolling back and the final history write.
*/}}
{{- define "duckfleet.termination" -}}
terminationGracePeriodSeconds: 630
{{- end -}}
{{- define "duckfleet.health" -}}
readinessProbe:
  httpGet: {path: /healthz, port: 8080}
  periodSeconds: 3
livenessProbe:
  httpGet: {path: /healthz, port: 8080}
  periodSeconds: 10
startupProbe:
  httpGet: {path: /healthz, port: 8080}
  periodSeconds: 5
  failureThreshold: 120
{{- end -}}

{{/* The values hash changes whenever the immutable Job template could change.
     runId also lets an operator rerun against a restored or retargeted database;
     Release.Revision is insufficient for helm template/kubectl apply installs. */}}
{{- define "duckfleet.migrationJob" -}}
{{- printf "duckfleet-platform-migrate-%s" (toJson .Values | sha256sum | trunc 12) -}}
{{- end -}}

{{/* Role names are namespaced even when several deployments share one PG cluster. */}}
{{- define "duckfleet.databaseRoles" -}}
{{- $prefix := printf "df_%s_%s" (.Release.Namespace | replace "-" "_" | trunc 36) (.Release.Namespace | sha256sum | trunc 8) -}}
{{- $roles := dict -}}
{{- $seen := dict -}}
{{- range $part := list "migrator" "gateway" "worker" "maintainer" "catalog" "catalog_reader" -}}
{{- $name := default (printf "%s_%s" $prefix $part) (index $.Values.databaseRoles $part) -}}
{{- if hasKey $seen $name -}}{{ fail "databaseRoles must name six distinct roles" }}{{- end -}}
{{- $_ := set $seen $name true -}}{{- $_ := set $roles $part $name -}}
{{- end -}}
{{ toJson $roles }}
{{- end -}}
{{- define "duckfleet.authorizationSeed" -}}
{{- $grant := .Values.authorization.defaultPersistentGrant -}}
{{- $pools := include "duckfleet.pools" . | fromJson -}}
{{- if ne $grant.pool .Values.defaultPool -}}{{ fail "authorization.defaultPersistentGrant.pool must match defaultPool; review the bounded upgrade grant explicitly" }}{{- end -}}
{{- $pool := index $pools $grant.pool -}}
{{- if or (lt (int $grant.cpu) (int $pool.cpu)) (lt (include "duckfleet.quantityBytes" $grant.memory | int64) (include "duckfleet.quantityBytes" $pool.memory | int64)) (lt (include "duckfleet.quantityBytes" $grant.disk | int64) (include "duckfleet.quantityBytes" $pool.disk | int64)) -}}
{{ fail "authorization.defaultPersistentGrant must cover the default allocation; review its explicit CPU/memory/disk ceilings" }}
{{- end -}}
- {name: DUCKFLEET_NAMESPACE, value: {{ .Release.Namespace | quote }}}
- {name: DUCKFLEET_AUTHZ_DEFAULT_POOL, value: {{ $grant.pool | quote }}}
- {name: DUCKFLEET_AUTHZ_DEFAULT_CPU, value: {{ $grant.cpu | quote }}}
- {name: DUCKFLEET_AUTHZ_DEFAULT_MEMORY, value: {{ $grant.memory | quote }}}
- {name: DUCKFLEET_AUTHZ_DEFAULT_DISK, value: {{ $grant.disk | quote }}}
{{- end -}}
{{- define "duckfleet.databaseProvisionEnvironment" -}}
{{- $roles := include "duckfleet.databaseRoles" . | fromJson -}}
{{- range $part := list "migrator" "gateway" "worker" "maintainer" "catalog" "catalog_reader" }}
- {name: DUCKFLEET_DB_{{ upper $part }}_ROLE, value: {{ index $roles $part | quote }}}
- name: DUCKFLEET_DB_{{ upper $part }}_PASSWORD
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: DATABASE_{{ upper $part }}_PASSWORD}
{{- end }}
- {name: DUCKFLEET_PLATFORM_DATABASE, value: platform}
- {name: DUCKFLEET_LAKE_DATABASE, value: lake}
{{- end -}}

{{/*
The isolated override, renamed to the worker's settings. maxThreads becomes the
worker's thread setting, which an isolated worker treats as a ceiling so one value
fits every requested size.
*/}}
{{- define "duckfleet.isolatedEngine" -}}
{{- $override := .Values.isolatedCompute.engineOverride | default dict -}}
{{- $engine := dict -}}
{{- if hasKey $override "memoryPercent" -}}{{- $_ := set $engine "memoryPercent" $override.memoryPercent -}}{{- end -}}
{{- if hasKey $override "maxThreads" -}}{{- $_ := set $engine "threads" $override.maxThreads -}}{{- end -}}
{{- toJson $engine -}}
{{- end -}}
