#!/usr/bin/env python3
"""Generate all Kubernetes manifests for the Video Streaming platform."""
import pathlib

BASE = pathlib.Path(__file__).resolve().parent.parent / "k8s"

files = {}

# ── upload-service ──────────────────────────────────────────────
files["upload-service/deployment.yaml"] = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: upload-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: upload-service
  template:
    metadata:
      labels:
        app: upload-service
    spec:
      containers:
        - name: upload-service
          image: video-streaming-app:latest
          imagePullPolicy: Never
          ports:
            - containerPort: 8080
          command: ["/bin/sh", "-c"]
          args:
            - >
              mvn -pl upload-service -DskipTests exec:java
              -Dexec.mainClass=com.distributed26.videostreaming.upload.upload.UploadServiceApplication
          env:
            - name: SERVICE_MODE
              value: "upload"
          envFrom:
            - configMapRef:
                name: video-streaming-config
            - secretRef:
                name: video-streaming-secrets
          startupProbe:
            httpGet:
              path: /health
              port: 8080
            failureThreshold: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8080
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              memory: "512Mi"
              cpu: "250m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
"""

files["upload-service/service.yaml"] = """\
apiVersion: v1
kind: Service
metadata:
  name: upload-service
spec:
  type: NodePort
  selector:
    app: upload-service
  ports:
    - port: 8080
      targetPort: 8080
      nodePort: 30080
"""

# ── status-service ──────────────────────────────────────────────
files["status-service/deployment.yaml"] = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: status-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: status-service
  template:
    metadata:
      labels:
        app: status-service
    spec:
      containers:
        - name: status-service
          image: video-streaming-app:latest
          imagePullPolicy: Never
          ports:
            - containerPort: 8081
          command: ["/bin/sh", "-c"]
          args:
            - >
              mvn -pl upload-service -DskipTests exec:java
              -Dexec.mainClass=com.distributed26.videostreaming.upload.upload.UploadServiceApplication
          env:
            - name: SERVICE_MODE
              value: "status"
          envFrom:
            - configMapRef:
                name: video-streaming-config
            - secretRef:
                name: video-streaming-secrets
          startupProbe:
            httpGet:
              path: /health
              port: 8081
            failureThreshold: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8081
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8081
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              memory: "512Mi"
              cpu: "250m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
"""

files["status-service/service.yaml"] = """\
apiVersion: v1
kind: Service
metadata:
  name: status-service
spec:
  type: NodePort
  selector:
    app: status-service
  ports:
    - port: 8081
      targetPort: 8081
      nodePort: 30081
"""

# ── processing-service ─────────────────────────────────────────
files["processing-service/deployment.yaml"] = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: processing-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: processing-service
  template:
    metadata:
      labels:
        app: processing-service
    spec:
      containers:
        - name: processing-service
          image: video-streaming-app:latest
          imagePullPolicy: Never
          ports:
            - containerPort: 8082
          command: ["/bin/sh", "-c"]
          args:
            - >
              mvn -pl processing-service -DskipTests exec:java
              -Dexec.mainClass=com.distributed26.videostreaming.processing.ProcessingServiceApplication
          env:
            - name: SERVICE_MODE
              value: "processing"
          envFrom:
            - configMapRef:
                name: video-streaming-config
            - secretRef:
                name: video-streaming-secrets
          volumeMounts:
            - name: processing-spool
              mountPath: /app/processing-spool
          startupProbe:
            httpGet:
              path: /health
              port: 8082
            failureThreshold: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8082
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8082
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              memory: "1Gi"
              cpu: "500m"
            limits:
              memory: "2Gi"
              cpu: "2000m"
      volumes:
        - name: processing-spool
          persistentVolumeClaim:
            claimName: processing-spool-pvc
"""

files["processing-service/service.yaml"] = """\
apiVersion: v1
kind: Service
metadata:
  name: processing-service
spec:
  type: ClusterIP
  selector:
    app: processing-service
  ports:
    - port: 8082
      targetPort: 8082
"""

files["processing-service/pvc.yaml"] = """\
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: processing-spool-pvc
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 5Gi
"""

# ── streaming-service ──────────────────────────────────────────
files["streaming-service/deployment.yaml"] = """\
apiVersion: apps/v1
kind: Deployment
metadata:
  name: streaming-service
spec:
  replicas: 2
  selector:
    matchLabels:
      app: streaming-service
  template:
    metadata:
      labels:
        app: streaming-service
    spec:
      containers:
        - name: streaming-service
          image: video-streaming-app:latest
          imagePullPolicy: Never
          ports:
            - containerPort: 8083
          command: ["/bin/sh", "-c"]
          args:
            - >
              mvn -pl streaming-service -DskipTests exec:java
              -Dexec.mainClass=com.distributed26.videostreaming.streaming.streaming.StreamingServiceApplication
          envFrom:
            - configMapRef:
                name: video-streaming-config
            - secretRef:
                name: video-streaming-secrets
          startupProbe:
            httpGet:
              path: /health
              port: 8083
            failureThreshold: 30
            periodSeconds: 10
          livenessProbe:
            httpGet:
              path: /health
              port: 8083
            initialDelaySeconds: 10
            periodSeconds: 15
          readinessProbe:
            httpGet:
              path: /health
              port: 8083
            initialDelaySeconds: 5
            periodSeconds: 5
          resources:
            requests:
              memory: "512Mi"
              cpu: "250m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
"""

files["streaming-service/service.yaml"] = """\
apiVersion: v1
kind: Service
metadata:
  name: streaming-service
spec:
  type: NodePort
  selector:
    app: streaming-service
  ports:
    - port: 8083
      targetPort: 8083
      nodePort: 30083
"""

# ── Write all files ────────────────────────────────────────────
for rel_path, content in files.items():
    path = BASE / rel_path
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    print(f"  Created: k8s/{rel_path}")

print(f"\nDone! {len(files)} manifest files written to k8s/")

