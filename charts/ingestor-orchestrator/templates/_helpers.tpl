{{/*
Expand the name of the chart.
*/}}
{{- define "ingestor-orchestrator.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "ingestor-orchestrator.fullname" -}}
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
{{- define "ingestor-orchestrator.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "ingestor-orchestrator.labels" -}}
helm.sh/chart: {{ include "ingestor-orchestrator.chart" . }}
{{ include "ingestor-orchestrator.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "ingestor-orchestrator.selectorLabels" -}}
app.kubernetes.io/name: {{ include "ingestor-orchestrator.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Resolve the image for a component.
Usage: {{ include "ingestor-orchestrator.image" (dict "root" . "component" "api") }}
*/}}
{{- define "ingestor-orchestrator.image" -}}
{{- $root := .root }}
{{- $comp := .component }}
{{- $compValues := index $root.Values $comp }}
{{- $registry := $compValues.image.registry | default $root.Values.global.image.registry }}
{{- $repository := $compValues.image.repository }}
{{- $tag := $compValues.image.tag | default $root.Values.global.image.tag }}
{{- $pullPolicy := $compValues.image.pullPolicy | default $root.Values.global.image.pullPolicy }}
{{- printf "%s/%s:%s" $registry $repository $tag }}
{{- end }}

{{/*
Name of the secret to use for environment variables.
*/}}
{{- define "ingestor-orchestrator.secretName" -}}
{{- if .Values.existingSecret }}
{{- .Values.existingSecret }}
{{- else }}
{{- include "ingestor-orchestrator.fullname" . }}
{{- end }}
{{- end }}

{{/*
Optional CNPG postgres secret envFrom.
Usage: {{ include "ingestor-orchestrator.postgresEnvFrom" . | nindent N }}
*/}}
{{- define "ingestor-orchestrator.postgresEnvFrom" -}}
{{- if .Values.postgres.deploy }}
- secretRef:
    name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
{{- end }}
{{- end }}

{{/*
CNPG DuckLake env vars — maps CNPG secret keys to DUCKLAKE_ prefix.
*/}}
{{- define "ingestor-orchestrator.cnpgDucklakeEnv" -}}
{{- if .Values.postgres.deploy }}
- name: DUCKLAKE_HOST
  valueFrom:
    secretKeyRef:
      name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
      key: host
- name: DUCKLAKE_PORT
  valueFrom:
    secretKeyRef:
      name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
      key: port
- name: DUCKLAKE_DBNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
      key: dbname
- name: DUCKLAKE_USERNAME
  valueFrom:
    secretKeyRef:
      name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
      key: username
- name: DUCKLAKE_PASSWORD
  valueFrom:
    secretKeyRef:
      name: {{ include "ingestor-orchestrator.fullname" . }}-postgres-app
      key: password
{{- end }}
{{- end }}
