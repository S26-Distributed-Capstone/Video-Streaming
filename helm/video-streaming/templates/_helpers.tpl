{{/*
Init container that waits for a TCP port to be reachable.
Prevents app pods from crash-looping while infra starts.
Usage: {{ include "video-streaming.wait-for" (dict "name" "rabbitmq" "host" (printf "%s-rabbitmq" $.Release.Name) "port" "5672") }}
*/}}
{{- define "video-streaming.wait-for" -}}
- name: wait-for-{{ .name }}
  image: busybox:1.36
  command: ["sh", "-c"]
  args:
    - |
      echo "Waiting for {{ .host }}:{{ .port }}..."
      until nc -z {{ .host }} {{ .port }} 2>/dev/null; do
        sleep 2
      done
      echo "{{ .host }}:{{ .port }} is up."
{{- end -}}

