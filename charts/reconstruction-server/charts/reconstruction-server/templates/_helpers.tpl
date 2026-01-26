{{/*
Expand the name of the chart.
*/}}
{{- define "reconstruction-server.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "reconstruction-server.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default .Chart.Name .Values.nameOverride }}
{{- if contains $name .Release.Name }}
{{- .Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "reconstruction-server.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "reconstruction-server.labels" -}}
helm.sh/chart: {{ include "reconstruction-server.chart" . }}
{{ include "reconstruction-server.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "reconstruction-server.selectorLabels" -}}
app.kubernetes.io/name: {{ include "reconstruction-server.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "reconstruction-server.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "reconstruction-server.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Validate configuration combinations.
*/}}
{{- define "reconstruction-server.validate" -}}
{{- $poolSize := int (default 0 .Values.secretPool.size) -}}
{{- if .Values.secretPool.enabled }}
  {{- if lt $poolSize 1 }}
    {{- fail "secretPool.size must be >= 1 when secretPool.enabled is true" }}
  {{- end }}
  {{- if not .Values.secretPool.namePrefix }}
    {{- fail "secretPool.namePrefix must be set when secretPool.enabled is true" }}
  {{- end }}
{{- end }}
{{- if .Values.autoscaling.hpa.enabled }}
  {{- $maxReplicas := int (default 0 .Values.autoscaling.hpa.maxReplicas) -}}
  {{- if and .Values.secretPool.enabled (gt $maxReplicas $poolSize) }}
    {{- fail "autoscaling.hpa.maxReplicas cannot exceed secretPool.size" }}
  {{- end }}
  {{- if and (not .Values.autoscaling.hpa.targetCPU) (not .Values.autoscaling.hpa.targetMemory) }}
    {{- fail "autoscaling.hpa requires targetCPU or targetMemory to be set" }}
  {{- end }}
{{- end }}
{{- end }}
