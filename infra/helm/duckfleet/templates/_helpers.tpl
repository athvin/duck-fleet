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
