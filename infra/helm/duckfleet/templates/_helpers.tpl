{{- define "duckfleet.containerSecurity" -}}
allowPrivilegeEscalation: false
capabilities:
  drop: [ALL]
readOnlyRootFilesystem: true
{{- end -}}
{{- define "duckfleet.podSecurity" -}}
runAsNonRoot: true
runAsUser: 10001
runAsGroup: 10001
fsGroup: 10001
seccompProfile:
  type: RuntimeDefault
{{- end -}}
{{/*
The worker classes this release runs, defined once. The gateway's endpoint list and
the Deployments it points at are both derived from this, so they cannot drift apart
and leave the gateway routing to a Service that was never rendered.
*/}}
{{/* Consumers must coerce the size with `int`: Helm parses this through JSON, so a
     number arrives as a float and `printf "%d"` on it renders invalid YAML. */}}
{{- define "duckfleet.workerClasses" -}}
small: 1
medium: 2
{{- end -}}
{{- define "duckfleet.workerUrls" -}}
{{- $urls := list -}}
{{- range $size, $cpu := include "duckfleet.workerClasses" . | fromYaml -}}
{{- $urls = append $urls (printf "http://worker-%s:8080" $size) -}}
{{- end -}}
{{- join "," $urls -}}
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
{{- define "duckfleet.workerEnv" -}}
{{- $ := .root -}}
- {name: DUCKFLEET_WORKER_ID, value: {{ .id | quote }}}
# Declared from the same values as the limits. The worker refuses to start if the
# container it actually got disagrees.
- {name: DUCKFLEET_WORKER_CPU, value: {{ .cpu | quote }}}
- {name: DUCKFLEET_WORKER_MEMORY, value: {{ .memory | quote }}}
- {name: S3_AUTH_MODE, value: pod_identity}
- {name: DUCKFLEET_PROFILING, value: {{ $.Values.profiling | quote }}}
- {name: DUCKFLEET_CATALOG_WAIT_SECONDS, value: {{ $.Values.catalogWaitSeconds | quote }}}
- {name: S3_DATA_PATH, value: {{ printf "s3://%s/lake/" $.Values.bucket | quote }}}
- {name: AWS_REGION, value: {{ $.Values.region }}}
- {name: AWS_EC2_METADATA_DISABLED, value: "true"}
- name: DUCKFLEET_TOKEN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: DUCKFLEET_TOKEN}
- name: CATALOG_DSN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: CATALOG_DSN}
- name: PLATFORM_DSN
  valueFrom:
    secretKeyRef: {name: {{ $.Values.secretName }}, key: PLATFORM_DSN}
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
  nodeSelector: {{ toJson .Values.nodeSelector }}
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
        {{- include "duckfleet.workerEnv" (dict "root" . "id" "replaced-per-session" "cpu" "1" "memory" "1Gi") | nindent 8 }}
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

It must exceed the longest statement a caller may ask for, which the API caps at 60
seconds. Unset it defaults to 30, so a query in its second half could never finish
however well the drain was written — the two numbers are coupled, and check-chart.py
asserts it so a later change to either fails rather than silently truncating drains.
The margin covers the engine rolling back and the final history write.
*/}}
{{- define "duckfleet.termination" -}}
terminationGracePeriodSeconds: 90
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
