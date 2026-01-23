{{/*
Expand the name of the chart.
*/}}
{{- define "strata.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "strata.fullname" -}}
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
{{- define "strata.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "strata.labels" -}}
helm.sh/chart: {{ include "strata.chart" . }}
{{ include "strata.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "strata.selectorLabels" -}}
app.kubernetes.io/name: {{ include "strata.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "strata.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- default (include "strata.fullname" .) .Values.serviceAccount.name }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Create the peer list for Raft cluster
*/}}
{{- define "strata.peerList" -}}
{{- $fullName := include "strata.fullname" . -}}
{{- $replicas := int .Values.replicaCount -}}
{{- $peers := list -}}
{{- range $i := until $replicas -}}
{{- $peers = append $peers (printf "%s-%d.%s-headless:%d" $fullName $i $fullName (int $.Values.service.metadata.port)) -}}
{{- end -}}
{{- join "," $peers -}}
{{- end }}

{{/*
Return the appropriate apiVersion for PodDisruptionBudget
*/}}
{{- define "strata.pdb.apiVersion" -}}
{{- if .Capabilities.APIVersions.Has "policy/v1" }}
{{- print "policy/v1" }}
{{- else }}
{{- print "policy/v1beta1" }}
{{- end }}
{{- end }}
