{{/*
Expand the name of the chart.
*/}}
{{- define "fluss.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "fluss.fullname" -}}
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

{{/*
Common labels
*/}}
{{- define "fluss.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name (.Chart.Version | replace "+" "_") | quote }}
app.kubernetes.io/name: {{ include "fluss.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service | quote }}
{{- end -}}

{{/*
Selector labels
*/}}
{{- define "fluss.selectorLabels" -}}
app.kubernetes.io/name: {{ include "fluss.name" . | quote }}
app.kubernetes.io/instance: {{ .Release.Name | quote }}
{{- end -}}

{{/* Service names */}}
{{- define "fluss.coordinator.serviceName" -}}
{{ include "fluss.fullname" . }}-coordinator
{{- end -}}

{{- define "fluss.tablet.headlessServiceName" -}}
{{ include "fluss.fullname" . }}-tablet-headless
{{- end -}}

{{- define "fluss.zookeeper.serviceName" -}}
{{- if .Values.zookeeper.service.name -}}
{{ .Values.zookeeper.service.name }}
{{- else -}}
zk
{{- end -}}
{{- end -}}
