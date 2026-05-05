{{/*
Standard helpers borrowed from the upstream `helm create` template
shape, plus a couple of crawlrs-specific helpers.

The label set follows the Kubernetes recommended labels
(app.kubernetes.io/*) so observability stacks and dashboarding tools
can group resources without having to know the chart name.
*/}}

{{/* Chart full name; truncated to 63 chars (k8s name limit). */}}
{{- define "crawlrs.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{- define "crawlrs.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "crawlrs.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/* Headless Service name. Used as `crawlrs-N.crawlrs-headless`. */}}
{{- define "crawlrs.headlessServiceName" -}}
{{- printf "%s-headless" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.configMapName" -}}
{{- printf "%s-config" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.secretName" -}}
{{- if .Values.secrets.existingSecret -}}
{{- .Values.secrets.existingSecret -}}
{{- else -}}
{{- printf "%s-secret" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}

{{/* Image reference. tag falls back to appVersion if values.tag is empty. */}}
{{- define "crawlrs.image" -}}
{{- $tag := .Values.image.tag | default .Chart.AppVersion -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/* Labels common to every workload. */}}
{{- define "crawlrs.labels" -}}
helm.sh/chart: {{ include "crawlrs.chart" . }}
{{ include "crawlrs.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
app.kubernetes.io/part-of: crawlrs
{{- end -}}

{{- define "crawlrs.selectorLabels" -}}
app.kubernetes.io/name: {{ include "crawlrs.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Render a single probe block. Usage:
  {{- include "crawlrs.probe" (dict "probe" .Values.probes.liveness "port" .Values.server.port) | nindent 10 }}
*/}}
{{- define "crawlrs.probe" -}}
httpGet:
  path: {{ .probe.path }}
  port: {{ .port }}
initialDelaySeconds: {{ .probe.initialDelaySeconds }}
periodSeconds: {{ .probe.periodSeconds }}
timeoutSeconds: {{ .probe.timeoutSeconds }}
failureThreshold: {{ .probe.failureThreshold }}
{{- end -}}

{{/* o11y helpers (Phase 6c). */}}
{{- define "crawlrs.vmsingleName" -}}
{{- printf "%s-vmsingle" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.vmsingleScrapeConfigMapName" -}}
{{- printf "%s-vmsingle-scrape" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.grafanaName" -}}
{{- printf "%s-grafana" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.grafanaDatasourceConfigMapName" -}}
{{- printf "%s-grafana-datasources" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.grafanaDashboardsProviderConfigMapName" -}}
{{- printf "%s-grafana-dashboards-provider" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{- define "crawlrs.grafanaDashboardsConfigMapName" -}}
{{- printf "%s-grafana-dashboards" (include "crawlrs.fullname" .) | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/* Component labels add app.kubernetes.io/component to the standard set. */}}
{{- define "crawlrs.componentLabels" -}}
{{- $component := .component -}}
{{- $context := .context -}}
{{ include "crawlrs.labels" $context }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}

{{- define "crawlrs.componentSelectorLabels" -}}
{{- $component := .component -}}
{{- $context := .context -}}
{{ include "crawlrs.selectorLabels" $context }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}
