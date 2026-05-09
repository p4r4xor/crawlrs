{{/*
Common labels applied to every demo-chart-owned resource (redis,
postgres, secrets). Component-specific labels are added per-template
via `app.kubernetes.io/component`.
*/}}
{{- define "crawlrs-demo.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels for matching pods to Services. A subset of the full
labels above; only the stable identity bits.
*/}}
{{- define "crawlrs-demo.selectorLabels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}
