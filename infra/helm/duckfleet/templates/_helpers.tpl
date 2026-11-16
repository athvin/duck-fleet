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
