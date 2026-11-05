# Current public code reference

Full contents of the public files in the report source manifest, as inspected on 5 November 2026. This includes pre-existing course helpers; it is an inventory, not an authorship claim. Runtime credentials and environment files are excluded. The original and November implementation reports retain the complete change patches.

## .dockerignore

Exclude local secrets, caches and irrelevant artifacts from the application build context.

SHA256: `a33dca25de9f8b464952d3c9c7d0f5c31f73090060d77824b8e8d0d60262c7c7`

````text
**
!requirements.lock
!temperature_observability/
!temperature_observability/**
!loadtests/
!loadtests/**
!deploy/
!deploy/Dockerfile
!deploy/Caddyfile
!deploy/docs.html
!experiment-producer/
!experiment-producer/schemas/
!experiment-producer/schemas/**
**/__pycache__/
# Public Rust harness sources, also needed by Docker's legacy builder (which
# does not support a Dockerfile-specific ignore file). Credentials stay excluded.
!Cargo.toml
!Cargo.lock
!.sqlx/
!.sqlx/**
!event-hash/
!event-hash/Cargo.toml
!event-hash/src/
!event-hash/src/**
!experiment-producer/Cargo.toml
!experiment-producer/src/
!experiment-producer/src/**
!notifications-service/
!notifications-service/Cargo.toml
!notifications-service/src/
!notifications-service/src/**
!notifications-service/ca.crt
!http-load-generator/
!http-load-generator/Cargo.toml
!http-load-generator/src/
!http-load-generator/src/**
!test-to-api/
!test-to-api/Cargo.toml
!test-to-api/src/
!test-to-api/src/**
!notifier/
!notifier/Cargo.toml
!notifier/src/
!notifier/src/**
````

## .gitignore

Keep private runtime configuration, credentials and generated test state out of version control.

SHA256: `5fdf4677cc8d3059735ef32fbcf9b7355a00ec378a5c1ae0409285aa01976771`

````text
# Local secrets and credentials
.env
.env.*
*.key
*.pem

# Local credentials
group15-credentials/
group-15-credentials/target/
ca.key
.env*
*.log*
auth/
.direnv/
result

# Local assignment implementation and credentials
group15-credentials/
group-15-credentials/
.venv/
__pycache__/
.pytest_cache/
.ruff_cache/
*.egg-info/
!.env.example
!monitoring.env.example
test-results/
.course-test/
````

## Makefile

Documented operator/test entry points, including variable tests and generator-only stopping.

SHA256: `9043f949c8ce2f1774e4ed2ae1cecbdc514e4dcb044897a2637374e6309fee23`

````text
ENVIRONMENT="data-consistency"

SERVICES=-f docker-compose.services.yml
TO=-f docker-compose.to.yml

.PHONY=run_producer run_http all

all: 

run_producer:
	./scripts/setup_env.sh $(ENVIRONMENT)
	docker compose $(SERVICES) pull
	docker compose $(SERVICES) up -d experiment-producer

run_http: 
	./scripts/setup_env.sh $(ENVIRONMENT)
	docker compose $(SERVICES) pull
	docker compose $(SERVICES) up -d http-load-generator

run_to: 
	./scripts/setup_env.sh $(ENVIRONMENT)
	docker compose $(TO) pull
	docker compose $(TO) up -d

MONITORING_COMPOSE=docker compose -f compose.yml -f compose.local.yml
PYTHON=.venv/bin/python

.PHONY: monitoring-build monitoring-up monitoring-down monitoring-metrics test integration stress recovery

monitoring-build:
	docker compose -f compose.yml build data api

monitoring-up: monitoring-build
	$(MONITORING_COMPOSE) up -d

monitoring-down:
	$(MONITORING_COMPOSE) down

monitoring-metrics:
	$(MONITORING_COMPOSE) -f compose.monitoring.yml up -d prometheus node-exporter grafana

test:
	$(PYTHON) -m pytest -q -m 'not integration'

integration:
	TO_TEST_STACK=1 $(PYTHON) -m pytest -q

stress:
	$(PYTHON) -m loadtests.stress --output test-results/stress.json

recovery:
	$(PYTHON) -m loadtests.recovery

.PHONY: course-test course-stress course-variable course-panel course-check course-stop course-kafka-check stress-variable

course-test:
	$(PYTHON) -m loadtests.course run

course-stress:
	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 60 --http-rps 100

course-variable:
	$(PYTHON) -m loadtests.course run --experiments 100 --sensors 10 --samples 80 --http-rps 100 --http-max-rps 200 --http-seconds 90

stress-variable:
	$(PYTHON) -m loadtests.stress --experiments 100 --sensors 10 --http-profile 50:15,200:30,100:15 --output test-results/variable.json

course-panel:
	$(PYTHON) -m loadtests.course up

course-check:
	$(PYTHON) -m loadtests.course check

course-stop:
	$(PYTHON) -m loadtests.course stop

course-kafka-check:
	$(PYTHON) scripts/check-course-kafka.py
````

## compose.course-test.yml

Original instructor binaries, local truth/receiver, variable-generator settings and local panel wiring.

SHA256: `9c72d0acb07f5819a753edea9abd5f5566455fde08adf0ac9c450e4ec837f93a`

````yaml
# Use with compose.yml and compose.local.yml. No course credentials are used.
x-course-tool: &course-tool
  image: cec-course-tools:local
  build:
    context: .
    dockerfile: deploy/CourseTools.Dockerfile
  init: true
  working_dir: /app
  environment:
    DATABASE_URL: postgresql://course:course-local@course-db:5432/course
    TOKIO_WORKER_THREADS: "2"
  logging:
    driver: json-file
    options: {max-size: 5m, max-file: "2"}

services:
  kafka:
    environment:
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:19092,TLS://:9094
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:19092,TLS://kafka:9094
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT,TLS:SSL
      KAFKA_SSL_KEYSTORE_TYPE: PKCS12
      KAFKA_SSL_KEYSTORE_LOCATION: /course-tls/server.pkcs12
      KAFKA_SSL_KEYSTORE_PASSWORD: cc2023
      KAFKA_SSL_KEY_PASSWORD: cc2023
      KAFKA_SSL_TRUSTSTORE_TYPE: PEM
      KAFKA_SSL_TRUSTSTORE_LOCATION: /course-tls/ca.crt
      KAFKA_SSL_CLIENT_AUTH: required
    volumes:
      - ./.course-test/broker:/course-tls:ro
  scheduler:
    environment:
      TO_KAFKA_TOPIC: course-test-events
      TO_KAFKA_GROUP: group15-course-test-v1
  notification:
    user: "${COURSE_UID:-1000}:${COURSE_GID:-1000}"
    environment:
      TO_NOTIFICATION_URL: http://course-notifications:3000/api/notify
      TO_NOTIFICATION_TOKEN_FILE: /run/course-token
    volumes:
      - ./.course-test/token:/run/course-token:ro
    depends_on:
      course-notifications: {condition: service_started}
  course-db:
    image: postgres:16-alpine
    environment:
      POSTGRES_USER: course
      POSTGRES_PASSWORD: course-local
      POSTGRES_DB: course
    volumes:
      - course-db-data:/var/lib/postgresql/data
      - ./deploy/course/schema.sql:/docker-entrypoint-initdb.d/course.sql:ro
    healthcheck:
      test: [CMD-SHELL, pg_isready -U course -d course]
      interval: 3s
      retries: 20
    restart: unless-stopped
  course-notifications:
    <<: *course-tool
    command: [notifications-service]
    env_file: ./.course-test/secret.env
    environment:
      DATABASE_URL: postgresql://course:course-local@course-db:5432/course
      RSA_PUBLIC_KEY: /run/course-jwt-public.pem
      TOKIO_WORKER_THREADS: "2"
    volumes:
      - ./.course-test/jwt-public.pem:/run/course-jwt-public.pem:ro
    ports: [127.0.0.1:3000:3000]
    depends_on:
      course-db: {condition: service_healthy}
    restart: unless-stopped
  course-producer:
    <<: *course-tool
    profiles: [course-load]
    command: [experiment-producer, --brokers, kafka:9094, --topic, course-test-events, --topic-document, course-test-documents, --config-file, /app/config.json]
    env_file: ./.course-test/secret.env
    volumes:
      - ./.course-test/client:/app/experiment-producer/auth:ro
      - ./.course-test/config.json:/app/config.json:ro
    ports: [127.0.0.1:3001:3001]
    depends_on:
      kafka: {condition: service_healthy}
      course-db: {condition: service_healthy}
  course-http:
    <<: *course-tool
    profiles: [course-load]
    command:
      - http-load-generator
      - --brokers
      - kafka:9094
      - --topic
      - course-test-documents
      - --group-id
      - ${COURSE_HTTP_GROUP:-group15-course-http-v1}
      - --consumer-wait-before-send
      - "3"
      - --hosts-file
      - /app/hosts.json
      - --min-batch-size
      - ${COURSE_HTTP_MIN:-100}
      - --max-batch-size
      - ${COURSE_HTTP_MAX:-101}
      - --num-generations
      - "255"
      - --stable-rate-duration
      - ${COURSE_HTTP_PHASE_SECONDS:-20}
      - --requestor-lag
      - "0"
      - --requestor-retries
      - "0"
      - --requestor-max-in-flight
      - "100"
    volumes:
      - ./.course-test/client:/app/auth:ro
      - ./.course-test/hosts.json:/app/hosts.json:ro
    ports: [127.0.0.1:3002:3002]
    depends_on:
      kafka: {condition: service_healthy}
      api: {condition: service_healthy}
  course-prometheus:
    image: prom/prometheus:v3.2.1
    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=7d, --storage.tsdb.retention.size=256MB]
    volumes:
      - ./deploy/course/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - course-prometheus-data:/prometheus
    ports: [127.0.0.1:3005:9090]
    restart: unless-stopped
  course-grafana:
    image: grafana/grafana:12.0.2
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_USERS_ALLOW_SIGN_UP: "false"
      GF_SECURITY_ADMIN_PASSWORD: ${COURSE_GRAFANA_PASSWORD:-course-local}
    volumes:
      - ./deploy/course/provisioning:/etc/grafana/provisioning:ro
      - ./.course-test/dashboards:/var/lib/grafana/course-dashboards:ro
      - course-grafana-data:/var/lib/grafana
    ports: [127.0.0.1:3009:3000]
    restart: unless-stopped
  node-exporter:
    image: prom/node-exporter:v1.9.1
    restart: unless-stopped
    pid: host
    command: [--path.rootfs=/host, --collector.textfile.directory=/course-metrics]
    volumes:
      - /:/host:ro,rslave
      - ./.course-test/metrics:/course-metrics:ro

volumes:
  course-db-data:
  course-prometheus-data:
  course-grafana-data:
````

## compose.demo.yml

External topic/group, mTLS mounts, known hostname exception and course notification destination.

SHA256: `a7f9aaa3c8251455b6faecbb19662e9a1859d8af6114b10ddf65e72e137fe91a`

````yaml
services:
  scheduler:
    user: "${TO_CREDENTIAL_UID:-1000}:${TO_CREDENTIAL_GID:-1000}"
    environment:
      TO_KAFKA_BROKERS: ${TO_KAFKA_BROKERS:-kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092}
      TO_KAFKA_TOPIC: ${TO_KAFKA_TOPIC:-experiment}
      TO_KAFKA_GROUP: ${TO_KAFKA_GROUP:-group15-temperature-demo-v1}
      TO_KAFKA_SECURITY_PROTOCOL: SSL
      # Course broker certificates use kafka-1/2/3, not the public DNS name.
      # Keep CA/client certificate verification; match the course tutorial.
      TO_KAFKA_ENDPOINT_IDENTIFICATION: ${TO_KAFKA_ENDPOINT_IDENTIFICATION:-none}
      TO_KAFKA_CA_FILE: /run/credentials/ca.crt
      TO_KAFKA_KEYSTORE_FILE: /run/credentials/kafka.keystore.pkcs12
      TO_KAFKA_KEYSTORE_PASSWORD_FILE: /run/credentials/keystore-password
    volumes:
      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/ca.crt:/run/credentials/ca.crt:ro
      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/kafka.keystore.pkcs12:/run/credentials/kafka.keystore.pkcs12:ro
      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/keystore-password:/run/credentials/keystore-password:ro
  notification:
    user: "${TO_CREDENTIAL_UID:-1000}:${TO_CREDENTIAL_GID:-1000}"
    environment:
      TO_NOTIFICATION_URL: https://notifications.cec.dlandau.nl/api/notify
      TO_NOTIFICATION_TOKEN_FILE: /run/credentials/token
    volumes:
      - ${TO_CREDENTIALS_DIR:-./group15-credentials}/token:/run/credentials/token:ro
````

## compose.local.yml

Independent local Kafka/sink wiring and bounded disposable broker retention.

SHA256: `2f270b396d1a978946faccb12de7f471924934c3ac1f17bea218c86a39b25519`

````yaml
services:
  kafka-init:
    image: apache/kafka:3.9.1
    user: root
    entrypoint: [sh, -c, "chown -R 1000:1000 /tmp/kraft-combined-logs"]
    volumes: [kafka-data:/tmp/kraft-combined-logs]
  kafka:
    image: apache/kafka:3.9.1
    restart: unless-stopped
    ports: [127.0.0.1:19092:19092]
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://:9092,CONTROLLER://:9093,HOST://:19092
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,HOST://localhost:19092
      KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,HOST:PLAINTEXT
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@kafka:9093
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
      KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
      KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 0
      KAFKA_NUM_PARTITIONS: 16
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
      KAFKA_LOG_RETENTION_HOURS: 24
      # Bound local test data as well as its age; retain the authoritative PG history.
      # Limits are per partition, and Kafka deletes whole, closed segments.
      KAFKA_LOG_RETENTION_BYTES: 33554432
      KAFKA_LOG_SEGMENT_BYTES: 16777216
      KAFKA_LOG_ROLL_MS: 300000
      KAFKA_LOG_RETENTION_CHECK_INTERVAL_MS: 60000
      KAFKA_LOG_DIRS: /tmp/kraft-combined-logs
      KAFKA_HEAP_OPTS: -Xms256m -Xmx384m
      CLUSTER_ID: MkU3OEVBNTcwNTJENDM2Qk
    volumes: [kafka-data:/tmp/kraft-combined-logs]
    depends_on:
      kafka-init: {condition: service_completed_successfully}
    healthcheck:
      test: [CMD, bash, -c, "exec 3<>/dev/tcp/localhost/9092"]
      interval: 10s
      timeout: 10s
      retries: 12
      start_period: 20s
  kafka-topic:
    image: apache/kafka:3.9.1
    entrypoint: [/opt/kafka/bin/kafka-topics.sh]
    command: [--bootstrap-server, kafka:9092, --create, --if-not-exists, --topic, experiment, --partitions, "16", --replication-factor, "1"]
    environment:
      KAFKA_HEAP_OPTS: -Xms64m -Xmx128m
    depends_on:
      kafka: {condition: service_healthy}
  notification-sink:
    image: cec-temperature:local
    build:
      context: .
      dockerfile: deploy/Dockerfile
      target: runtime
    command: [uvicorn, loadtests.notification_sink:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
    ports: [127.0.0.1:18080:8000]
    restart: unless-stopped
  scheduler:
    environment:
      TO_KAFKA_BROKERS: kafka:9092
      TO_KAFKA_SECURITY_PROTOCOL: PLAINTEXT
      TO_KAFKA_TOPIC: experiment
    depends_on:
      kafka: {condition: service_healthy}
      kafka-topic: {condition: service_completed_successfully}
  notification:
    environment:
      TO_NOTIFICATION_URL: http://notification-sink:8000/api/notify
      TO_NOTIFICATION_TOKEN_FILE: ""
  data:
    ports: [127.0.0.1:8002:8000]
  postgres:
    ports: [127.0.0.1:15432:5432]
  redis:
    ports: [127.0.0.1:16379:6379]

volumes:
  kafka-data:
````

## compose.monitoring.yml

Operational Prometheus/Grafana/node exporter, provisioning and retention limits.

SHA256: `c589a898dbeaab4f2aee0bcd6d55879496c77d9653d3efeab865bf1221fb8565`

````yaml
services:
  prometheus:
    image: prom/prometheus:v3.2.1
    restart: unless-stopped
    ports: ["3008:9090"]
    command: [--config.file=/etc/prometheus/prometheus.yml, --storage.tsdb.retention.time=15d, --storage.tsdb.retention.size=512MB]
    volumes:
      - ./deploy/prometheus.yml:/etc/prometheus/prometheus.yml:ro
      - ./deploy/alerts.yml:/etc/prometheus/alerts.yml:ro
      - prometheus-data:/prometheus
  node-exporter:
    image: prom/node-exporter:v1.9.1
    restart: unless-stopped
    pid: host
    command: [--path.rootfs=/host]
    volumes: ["/:/host:ro,rslave"]
  grafana:
    image: grafana/grafana:12.0.2
    restart: unless-stopped
    ports: [127.0.0.1:3004:3000]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Viewer
      GF_SECURITY_ADMIN_PASSWORD: ${TO_GRAFANA_PASSWORD:-local-demo}
      GF_USERS_ALLOW_SIGN_UP: "false"
    volumes:
      - ./deploy/grafana/provisioning:/etc/grafana/provisioning:ro
      - ./deploy/grafana/dashboards:/var/lib/grafana/dashboards:ro
      - grafana-data:/var/lib/grafana
volumes:
  prometheus-data:
  grafana-data:
````

## compose.yml

Core service boundaries, health/dependency wiring, durable volumes, limits and capped logs.

SHA256: `04144e3e9ae90912abaabcad0eb6d12a1cfdae17d73686813b42f8a1eb03ab39`

````yaml
name: cec-temperature

x-logging: &logging
  driver: json-file
  options: {max-size: 5m, max-file: "3"}

x-python: &python
  image: cec-temperature:local
  build:
    context: .
    dockerfile: deploy/Dockerfile
    target: runtime
  init: true
  restart: unless-stopped
  stop_grace_period: 30s
  logging: *logging
  healthcheck:
    test: [CMD, python, -m, temperature_observability.healthcheck]
    interval: 30s
    timeout: 4s
    retries: 12
    start_period: 15s

services:
  postgres:
    image: postgres:16-alpine
    restart: unless-stopped
    logging: *logging
    environment:
      POSTGRES_DB: observability
      POSTGRES_USER: observability
      POSTGRES_PASSWORD: ${TO_POSTGRES_PASSWORD:-observability}
    volumes: [postgres-data:/var/lib/postgresql/data]
    healthcheck:
      test: [CMD-SHELL, pg_isready -U observability -d observability]
      interval: 3s
      timeout: 3s
      retries: 20
  redis:
    image: redis:7.4-alpine
    restart: unless-stopped
    logging: *logging
    command: [redis-server, --maxmemory, 128mb, --maxmemory-policy, allkeys-lru, --save, ""]
    healthcheck:
      test: [CMD, redis-cli, ping]
      interval: 3s
      timeout: 3s
      retries: 20
  data:
    <<: *python
    command: [uvicorn, temperature_observability.data:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
    environment:
      TO_DATABASE_URL: postgresql://observability:${TO_POSTGRES_PASSWORD:-observability}@postgres:5432/observability
      TO_REDIS_URL: redis://redis:6379/0
      TO_VM_HOURLY_COST_USD: ${TO_VM_HOURLY_COST_USD:-0}
      TO_STORAGE_GB_MONTHLY_COST_USD: ${TO_STORAGE_GB_MONTHLY_COST_USD:-0}
      TO_STORAGE_PROVISIONED_GB: ${TO_STORAGE_PROVISIONED_GB:-0}
      TO_HISTORY_CACHE_POINTS: ${TO_HISTORY_CACHE_POINTS:-16384}
    depends_on:
      postgres: {condition: service_healthy}
      redis: {condition: service_healthy}
  executor:
    <<: *python
    command: [uvicorn, temperature_observability.executor:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
    environment:
      TO_DATA_URL: http://data:8000
      TO_EXECUTOR_CAPACITY: ${TO_EXECUTOR_CAPACITY:-8}
      TO_MAX_PENDING_MEASUREMENTS: ${TO_MAX_PENDING_MEASUREMENTS:-256}
    deploy:
      replicas: ${TO_EXECUTOR_REPLICAS:-1}
    depends_on:
      data: {condition: service_healthy}
  scheduler:
    <<: *python
    command: [uvicorn, temperature_observability.scheduler:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
    ports: [127.0.0.1:8001:8000]
    environment:
      TO_KAFKA_BROKERS: ${TO_KAFKA_BROKERS:-kafka:9092}
      TO_KAFKA_TOPIC: ${TO_KAFKA_TOPIC:-experiment}
      TO_KAFKA_GROUP: ${TO_KAFKA_GROUP:-group15-temperature-v1}
      TO_KAFKA_SECURITY_PROTOCOL: ${TO_KAFKA_SECURITY_PROTOCOL:-PLAINTEXT}
      TO_KAFKA_CA_FILE: ${TO_KAFKA_CA_FILE:-}
      TO_KAFKA_KEYSTORE_FILE: ${TO_KAFKA_KEYSTORE_FILE:-}
      TO_KAFKA_KEYSTORE_PASSWORD_FILE: ${TO_KAFKA_KEYSTORE_PASSWORD_FILE:-}
      TO_KAFKA_ENDPOINT_IDENTIFICATION: ${TO_KAFKA_ENDPOINT_IDENTIFICATION:-https}
      TO_BATCH_SIZE: ${TO_BATCH_SIZE:-256}
      TO_QUEUE_CAPACITY: ${TO_QUEUE_CAPACITY:-8192}
    depends_on:
      executor: {condition: service_healthy}
  api:
    image: cec-temperature-gateway:local
    build:
      context: .
      dockerfile: deploy/Dockerfile
      target: gateway
    restart: unless-stopped
    logging: *logging
    ports: ["3003:8000"]
    healthcheck:
      test: [CMD, wget, -q, -O, /dev/null, http://127.0.0.1:8000/health]
      interval: 5s
      timeout: 3s
      retries: 12
    depends_on:
      data: {condition: service_healthy}
      executor: {condition: service_healthy}
  notification:
    <<: *python
    command: [uvicorn, temperature_observability.notification:app, --host, 0.0.0.0, --port, "8000", --no-access-log]
    environment:
      TO_NOTIFICATION_URL: ${TO_NOTIFICATION_URL:-http://notification-sink:8000/api/notify}
      TO_NOTIFICATION_TOKEN_FILE: ${TO_NOTIFICATION_TOKEN_FILE:-}
      TO_NOTIFICATION_INTERVAL: ${TO_NOTIFICATION_INTERVAL:-0.1}
      TO_NOTIFICATION_CONCURRENCY: ${TO_NOTIFICATION_CONCURRENCY:-32}
      TO_NOTIFICATION_TIMEOUT: ${TO_NOTIFICATION_TIMEOUT:-2}
      TO_NOTIFICATION_LEASE: ${TO_NOTIFICATION_LEASE:-15}
    depends_on:
      data: {condition: service_healthy}

volumes:
  postgres-data:
````

## deploy/Caddyfile

Public-only route forwarding and gateway behavior; internal mutations are not exposed.

SHA256: `f96c8f740930d7547558bba78efc0fe14aa4ce6375c8bbd0691967bdac125aa4`

````text
{
	admin off
	auto_https off
	servers {
		metrics
	}
}

:8000 {
	handle /metrics {
		metrics
	}
	@queries path /temperature /temperature/out-of-range /temperature/out-of-range/intervals /experiments/* /health
	handle @queries {
		reverse_proxy data:8000 {
			transport http {
				dial_timeout 2s
				response_header_timeout 10s
			}
		}
	}
	handle /experiments {
		rewrite * /configure
		reverse_proxy executor:8000 {
			transport http {
				dial_timeout 2s
				response_header_timeout 10s
			}
		}
	}
	handle /docs {
		root * /srv
		rewrite * /docs.html
		file_server
	}
	handle /openapi.json {
		root * /srv
		file_server
	}
	handle {
		header Content-Type application/json
		respond `{"detail":"not found"}` 404
	}
	handle_errors {
		header Content-Type application/json
		respond `{"detail":"monitoring service temporarily unavailable"}` 503
	}
}
````

## deploy/CourseTools.Dockerfile

Build unmodified instructor Rust binaries for independent validation.

SHA256: `00e5f162af8094797fd0e7a6d121f82d31acbda111e3574534821785898cf2f2`

````text
FROM rust:1.90.0-bookworm AS builder
RUN apt-get update && apt-get install -y --no-install-recommends cmake libssl-dev pkg-config && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY .sqlx ./.sqlx
COPY event-hash ./event-hash
COPY experiment-producer ./experiment-producer
COPY notifications-service ./notifications-service
COPY http-load-generator ./http-load-generator
COPY test-to-api ./test-to-api
COPY notifier ./notifier
ENV SQLX_OFFLINE=true
RUN cargo build --locked --release -j 1 --bin experiment-producer --bin notifications-service --bin http-load-generator

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates libssl3 && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY --from=builder /app/target/release/experiment-producer /app/target/release/notifications-service /app/target/release/http-load-generator /usr/local/bin/
COPY experiment-producer/schemas ./experiment-producer/schemas
RUN mkdir -p notifications-service && touch experiment-producer/.env notifications-service/.env
````

## deploy/CourseTools.Dockerfile.dockerignore

Restrict the instructor-tool build context; avoid including private/generated state.

SHA256: `ca756dab7bfaaae4a7e734e619d1f2153e9081b81c4969a82938be980673531e`

````text
**
!Cargo.toml
!Cargo.lock
!.sqlx/
!.sqlx/**
!event-hash/
!event-hash/Cargo.toml
!event-hash/src/
!event-hash/src/**
!experiment-producer/
!experiment-producer/Cargo.toml
!experiment-producer/src/
!experiment-producer/src/**
!experiment-producer/schemas/
!experiment-producer/schemas/**
!notifications-service/
!notifications-service/Cargo.toml
!notifications-service/src/
!notifications-service/src/**
!notifications-service/ca.crt
!http-load-generator/
!http-load-generator/Cargo.toml
!http-load-generator/src/
!http-load-generator/src/**
!test-to-api/
!test-to-api/Cargo.toml
!test-to-api/src/
!test-to-api/src/**
!notifier/
!notifier/Cargo.toml
!notifier/src/
!notifier/src/**
````

## deploy/Dockerfile

Application and gateway build stages; repeatable Python runtime and OpenAPI artifacts.

SHA256: `a007a0e8b4466273722c75867b83d806b29061cfea5d8dab8791cf7f33790f3c`

````text
FROM python:3.12-slim AS runtime
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir -r requirements.lock \
    && useradd --create-home --uid 10001 app
COPY temperature_observability ./temperature_observability
COPY loadtests ./loadtests
COPY experiment-producer/schemas ./experiment-producer/schemas
RUN python -c 'import json; from temperature_observability.api import app; print(json.dumps(app.openapi()))' > /app/openapi.json
USER app
EXPOSE 8000
CMD ["uvicorn", "temperature_observability.api:app", "--host", "0.0.0.0", "--port", "8000", "--no-access-log"]

FROM caddy:2.10.2-alpine AS gateway
COPY deploy/Caddyfile /etc/caddy/Caddyfile
COPY deploy/docs.html /srv/docs.html
COPY --from=runtime /app/openapi.json /srv/openapi.json
EXPOSE 8000
````

## deploy/alerts.yml

Nine validated rules covering service/jobs/input, individual deadlines, pool wait and disk pressure.

SHA256: `b4b641a1bcd408c82cf69be0105f67728c5934497c0d633a703e9baa59ff4f06`

````yaml
groups:
  - name: temperature-observability
    rules:
      - alert: NotificationDeadlineAtRisk
        expr: to_notification_oldest_pending_seconds > 5
        labels: {severity: warning}
        annotations:
          summary: Pending alerts are approaching the 10-second deadline
      - alert: NotificationLatencyExceeded
        expr: increase(to_notification_deadline_misses_total[5m]) > 0
        labels: {severity: critical}
        annotations:
          summary: At least one accepted notification missed the strict under-10-second deadline
      - alert: DatabasePoolSaturated
        expr: histogram_quantile(0.95, sum by (le) (rate(to_database_pool_wait_seconds_bucket[2m]))) > 0.1
        for: 30s
        labels: {severity: warning}
        annotations:
          summary: Database connection waits are slowing requests; inspect pool and transaction timings
      - alert: DiskSpaceLow
        expr: node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes < 0.15
        for: 1m
        labels: {severity: warning}
        annotations:
          summary: Less than 15% free disk; preserve PostgreSQL volumes when freeing space
      - alert: DiskSpaceCritical
        expr: node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"} / node_filesystem_size_bytes < 0.08
        for: 30s
        labels: {severity: critical}
        annotations:
          summary: Less than 8% free disk; add space or remove regenerable images and build cache
      - alert: DiskWillFillWithinOneDay
        expr: predict_linear(node_filesystem_avail_bytes{mountpoint="/",fstype!~"tmpfs|overlay"}[1h], 86400) < 0
        for: 10m
        labels: {severity: warning}
        annotations:
          summary: Current storage growth will exhaust disk within 24 hours
      - alert: RejectedNotification
        expr: to_notification_jobs{status="failed"} > 0
        labels: {severity: critical}
        annotations:
          summary: The notification API rejected a durable job; inspect notification_outbox
      - alert: InvalidExperimentEvent
        expr: delta(to_rejected_events[5m]) > 0
        labels: {severity: warning}
        annotations:
          summary: Inspect rejected_events for invalid or incomplete experiment input
      - alert: ServiceUnavailable
        expr: up == 0
        for: 30s
        labels: {severity: critical}
        annotations:
          summary: A monitoring service cannot be scraped
````

## deploy/course/prometheus.yml

Scrape local instructor metrics and explicit run status.

SHA256: `4307bec03bbb2b8076db1de320a60311f9c5f1a6836e8b27242baaf51a0cb428`

````yaml
global:
  scrape_interval: 1s
scrape_configs:
  - job_name: course-notifications
    metrics_path: /api/metrics
    static_configs:
      - targets: [course-notifications:3000]
  - job_name: course-producer
    static_configs:
      - targets: [course-producer:3001]
  - job_name: course-http
    static_configs:
      - targets: [course-http:3002]
  - job_name: infrastructure-costs
    static_configs:
      - targets: [node-exporter:9100]
        labels: {instance: "group15:9100"}
````

## deploy/course/provisioning/dashboards/course.yml

Provision local course dashboard location.

SHA256: `53449f316fed19ced38b49ac4866830d6a801e65f7c9d74f10e17c7e94678d06`

````yaml
apiVersion: 1
providers:
  - name: Instructor grading dashboard
    folder: Course tests
    type: file
    options:
      path: /var/lib/grafana/course-dashboards
````

## deploy/course/provisioning/datasources/course.yml

Provision local course Prometheus and truth PostgreSQL datasources.

SHA256: `1b250ef2d83ae585d6a3bcec596485aac023dcfe4710b1575468adde8378b117`

````yaml
apiVersion: 1
datasources:
  - name: Course Prometheus
    uid: course-prometheus
    type: prometheus
    access: proxy
    url: http://course-prometheus:9090
    isDefault: true
    editable: false
  - name: Course PostgreSQL
    uid: course-postgres
    type: grafana-postgresql-datasource
    url: course-db:5432
    user: course
    jsonData:
      database: course
      sslmode: disable
      postgresVersion: 1600
    secureJsonData:
      password: course-local
    editable: false
````

## deploy/course/schema.sql

Separate original-validator truth/notification tables; not the application authority.

SHA256: `56c15681fa0c4e48bd66a11e0d364e7c588a7751ec5194ef02a28e464ad39617`

````sql
CREATE SCHEMA IF NOT EXISTS demo;
CREATE TABLE IF NOT EXISTS demo.notification_ground_truth (
    experiment_id TEXT NOT NULL,
    measurement_id TEXT NOT NULL,
    insert_timestamp TIMESTAMP DEFAULT now(),
    PRIMARY KEY (experiment_id, measurement_id)
);
CREATE TABLE IF NOT EXISTS demo.notification (
    experiment_id TEXT NOT NULL,
    measurement_id TEXT NOT NULL,
    group_id TEXT NOT NULL,
    latency DOUBLE PRECISION NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id, group_id)
);
````

## deploy/docs.html

Public API documentation page served by the gateway.

SHA256: `6ff6ebac412d5eed8bdc7a177f451bc9db2b094557846d920f6d96412f774bd0`

````html
<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>XYZ Lab Temperature Observability API</title>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
</head>
<body>
  <div id="swagger-ui"></div>
  <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
  <script>SwaggerUIBundle({url: '/openapi.json', dom_id: '#swagger-ui'});</script>
</body>
</html>
````

## deploy/grafana/dashboards/temperature.json

Operational dashboard, including pool, cache, disk, costs and individual deadline misses.

SHA256: `814e2d8e0d9a2560ca598ac1ba0541b3999a329c4ac07e7072c7d1a92b3de591`

````json
{
  "uid": "xyz-temperature",
  "title": "XYZ Lab \u2014 Temperature Observability",
  "schemaVersion": 40,
  "version": 2,
  "refresh": "10s",
  "time": {
    "from": "now-30m",
    "to": "now"
  },
  "tags": [
    "cec",
    "temperature"
  ],
  "panels": [
    {
      "id": 1,
      "title": "Kafka events processed / second",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 0,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "sum(rate(to_kafka_processed_total[1m]))",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 2,
      "title": "Scheduler queue",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 0,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_scheduler_queued_events",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 3,
      "title": "Notification latency p99",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 8,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.99, sum by(le) (rate(to_notification_latency_seconds_bucket[5m])))",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 4,
      "title": "Oldest pending notification",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 8,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_notification_oldest_pending_seconds",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 5,
      "title": "HTTP requests / second",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 16,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "sum(rate(caddy_http_requests_total{job=\"api\",handler=\"subroute\"}[1m]))",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "reqps"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 6,
      "title": "HTTP latency p95",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 16,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by(le) (rate(caddy_http_request_duration_seconds_bucket{job=\"api\",handler=\"subroute\"}[5m])))",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 7,
      "title": "Notification jobs",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 24,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_notification_jobs",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 8,
      "title": "Executor tasks and replicas",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 24,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_executor_active_tasks",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 9,
      "title": "Process resident memory",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 32,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "process_resident_memory_bytes{job!=\"node\"}",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bytes"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 10,
      "title": "VM CPU utilization",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 32,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "1 - avg(rate(node_cpu_seconds_total{mode=\"idle\"}[5m]))",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percentunit"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 11,
      "title": "Database size",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 40,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_database_bytes",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bytes"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 12,
      "title": "Cache hit rate",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 40,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "sum(rate(to_cache_reads_total{result=~\"hit|snapshot_hit\"}[5m])) / clamp_min(sum(rate(to_cache_reads_total{result=~\"hit|miss|snapshot_hit|snapshot_fill\"}[5m])), 1e-9)",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "percentunit"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 13,
      "title": "Estimated infrastructure cost / hour (configure rates)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 48,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_estimated_hourly_cost_usd",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "currencyUSD"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 14,
      "title": "Estimated cost over the last 24h",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 48,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "sum_over_time(to_estimated_hourly_cost_usd[24h]) * 15 / 3600",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "currencyUSD"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 15,
      "title": "Cost rate configured (1 = yes)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 56,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_cost_configured",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 16,
      "title": "Rejected input events",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 56,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_rejected_events",
          "legendFormat": "{{job}} {{instance}} {{status}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 17,
      "title": "Database pool wait p95 by operation",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 64,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by (le,operation) (rate(to_database_pool_wait_seconds_bucket[2m])))",
          "legendFormat": "{{operation}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 18,
      "title": "Database connection occupancy p95 by operation",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 64,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "histogram_quantile(0.95, sum by (le,operation) (rate(to_database_connection_seconds_bucket[2m])))",
          "legendFormat": "{{operation}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "s"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 19,
      "title": "Database pool connections",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 72,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "to_database_pool_connections",
          "legendFormat": "{{state}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 20,
      "title": "Notifications missing the strict deadline (5m)",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 72,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "increase(to_notification_deadline_misses_total[5m])",
          "legendFormat": "{{instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "short"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 21,
      "title": "Free root filesystem space",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 0,
        "y": 80,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "node_filesystem_avail_bytes{mountpoint=\"/\",fstype!~\"tmpfs|overlay\"}",
          "legendFormat": "{{instance}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "bytes"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    },
    {
      "id": 22,
      "title": "Data cache outcomes / second",
      "type": "timeseries",
      "datasource": {
        "type": "prometheus",
        "uid": "prometheus"
      },
      "gridPos": {
        "x": 12,
        "y": 80,
        "w": 12,
        "h": 8
      },
      "targets": [
        {
          "refId": "A",
          "expr": "sum by (result) (rate(to_cache_reads_total[2m]))",
          "legendFormat": "{{result}}"
        }
      ],
      "fieldConfig": {
        "defaults": {
          "unit": "ops"
        },
        "overrides": []
      },
      "options": {
        "legend": {
          "displayMode": "list",
          "placement": "bottom"
        }
      }
    }
  ]
}
````

## deploy/grafana/provisioning/dashboards/default.yml

Provision operational dashboard location.

SHA256: `076cb56000ccadbf5ccd9d7bf24869ca8e31f82f5ba10d143f5475dc9d09b5f8`

````yaml
apiVersion: 1
providers:
  - name: XYZ Lab
    folder: Temperature Observability
    type: file
    options:
      path: /var/lib/grafana/dashboards
````

## deploy/grafana/provisioning/datasources/prometheus.yml

Provision operational Prometheus datasource.

SHA256: `725d7ae42e32230bc315c45b09d250332d8617b5c65b03e4da906c669fe20905`

````yaml
apiVersion: 1
datasources:
  - name: Prometheus
    uid: prometheus
    type: prometheus
    access: proxy
    url: http://prometheus:9090
    isDefault: true
    editable: false
````

## deploy/prometheus.yml

Operational service/node scrape targets and alert rule loading.

SHA256: `0a8269827fd9bcaee00ff37c6c1f677141b0044061ee1382c882518c3b3f5ae6`

````yaml
global:
  scrape_interval: 15s
  evaluation_interval: 15s
rule_files: [/etc/prometheus/alerts.yml]
scrape_configs:
  - job_name: api
    static_configs:
      - targets: [api:8000]
  - job_name: data
    static_configs:
      - targets: [data:8000]
  - job_name: scheduler
    static_configs:
      - targets: [scheduler:8000]
  - job_name: notification
    static_configs:
      - targets: [notification:8000]
  - job_name: executor
    dns_sd_configs:
      - names: [executor]
        type: A
        port: 8000
        refresh_interval: 5s
  - job_name: node
    static_configs:
      - targets: [node-exporter:9100]
````

## loadtests/\_\_init\_\_.py

Package marker; no standalone service behavior.

SHA256: `5408d218d1bb59fa006cde2fe1ddbd9c5eff923168f398f280fad3b049a2d480`

````python
"""Local, isolated workload generation and validation."""
````

## loadtests/course.py

Original Rust self-test orchestration, ground-truth checks, variable phase evidence, panels and source/image build reuse.

SHA256: `1e5df34614bcce940cf2ca65207d0ed933edd43b26218be9e56d1140c03a007a`

````python
"""Run the repository's Rust instructor validators against our Python service."""

import argparse
import base64
import hashlib
import io
import json
import os
import secrets
import shlex
import shutil
import subprocess
import time
import tomllib
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime
from pathlib import Path

import fastavro
from confluent_kafka import Consumer, TopicPartition
from prometheus_client.parser import text_string_to_metric_families

from .validation import meets_notification_deadline, variable_http_phases

ROOT = Path(__file__).resolve().parents[1]
STATE = ROOT / ".course-test"
RESULTS = ROOT / "test-results"
LATEST = RESULTS / "course-latest.json"


def write(path, content, private=False):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content)
    path.chmod(0o600 if private else 0o644)


def openssl(*args, input=None):
    return subprocess.run(
        ["openssl", *map(str, args)], input=input, check=True, capture_output=True
    ).stdout


def prepare(args):
    STATE.mkdir(exist_ok=True)
    private = STATE / "private"
    private.mkdir(exist_ok=True, mode=0o700)
    for name in ("client", "broker", "dashboards", "metrics"):
        (STATE / name).mkdir(exist_ok=True)
    ca = private / "ca.crt"
    ca_key = private / "ca.key"
    if not ca.exists():
        openssl(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "365",
            "-subj",
            "/CN=CEC local test CA",
            "-addext",
            "basicConstraints=critical,CA:TRUE",
            "-keyout",
            ca_key,
            "-out",
            ca,
        )
        ca_key.chmod(0o600)
    for name, usage, output in (
        ("server", "serverAuth", STATE / "broker/server.pkcs12"),
        ("client", "clientAuth", STATE / "client/kafka.keystore.pkcs12"),
    ):
        if not output.exists():
            key, csr, cert, ext = [
                private / f"{name}.{suffix}" for suffix in ("key", "csr", "crt", "ext")
            ]
            openssl(
                "req",
                "-new",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                f"/CN={name}",
                "-keyout",
                key,
                "-out",
                csr,
            )
            write(
                ext,
                "basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\n"
                f"extendedKeyUsage={usage}\nsubjectAltName=DNS:kafka,DNS:localhost,IP:127.0.0.1\n",
            )
            openssl(
                "x509",
                "-req",
                "-in",
                csr,
                "-CA",
                ca,
                "-CAkey",
                ca_key,
                "-set_serial",
                "0x" + secrets.token_hex(16),
                "-days",
                "365",
                "-extfile",
                ext,
                "-out",
                cert,
            )
            # The unmodified instructor Kafka clients require this test password.
            openssl(
                "pkcs12",
                "-export",
                "-name",
                name,
                "-inkey",
                key,
                "-in",
                cert,
                "-certfile",
                ca,
                "-out",
                output,
                "-passout",
                "pass:cc2023",
            )
            key.chmod(0o600)
            output.chmod(0o600)
    for target in (STATE / "broker/ca.crt", STATE / "client/ca.crt"):
        shutil.copyfile(ca, target)
    jwt_key = private / "jwt.key"
    if not jwt_key.exists():
        openssl("genrsa", "-out", jwt_key, "2048")
        jwt_key.chmod(0o600)
    openssl("rsa", "-in", jwt_key, "-pubout", "-out", STATE / "jwt-public.pem")

    def b64(data):
        return base64.urlsafe_b64encode(data).rstrip(b"=")

    header = b64(b'{"alg":"RS256","typ":"JWT"}')
    claims = b64(json.dumps({"sub": "group15", "exp": int(time.time()) + 86400 * 30}).encode())
    signed = header + b"." + claims
    signature = openssl("dgst", "-sha256", "-sign", jwt_key, input=signed)
    write(STATE / "token", (signed + b"." + b64(signature)).decode() + "\n", private=True)
    if not (STATE / "secret.env").exists():
        write(STATE / "secret.env", "SECRET_KEY=" + secrets.token_hex(16) + "\n", private=True)

    def experiment(start=0, sensors=None, stabilization=None, samples=None):
        return {
            "start_time": start,
            "researcher": "course-test@example.invalid",
            "num_sensors": sensors or args.sensors,
            "sample_rate": 1000,
            "start_temperature": 0,
            "stabilization_samples": stabilization or 10,
            "carry_out_samples": samples or args.samples,
            "temp_range": {"lower_threshold": 20, "upper_threshold": 25},
        }

    # A short completed experiment gives the HTTP tester ground truth before the
    # main workload starts. At most args.experiments run concurrently.
    config = [experiment(sensors=2, stabilization=5, samples=5)]
    config.extend(experiment(start=15) for _ in range(args.experiments))
    write(STATE / "config.json", json.dumps(config, indent=2) + "\n")
    write(STATE / "hosts.json", '[{"host_name":"group15","base_url":"http://api:8000"}]\n')
    run_id = str(time.time_ns())
    write(
        STATE / "run.env",
        f"COURSE_HTTP_GROUP=group15-course-http-{run_id}\n"
        f"COURSE_HTTP_MIN={args.http_rps}\nCOURSE_HTTP_MAX={(args.http_max_rps or args.http_rps) + 1}\n"
        f"COURSE_HTTP_PHASE_SECONDS={args.http_phase_seconds}\n"
        f"COURSE_UID={os.getuid()}\nCOURSE_GID={os.getgid()}\n",
    )

    text = (ROOT / "observability/group_view.json").read_text()
    text = text.replace("${DS_PROMETHEUS}", "course-prometheus")
    text = text.replace("${DS_POSTGRESQL}", "course-postgres")
    dashboard = json.loads(text)
    dashboard.pop("__inputs", None)
    dashboard["id"] = None
    dashboard["uid"] = "cec-course-grading"
    dashboard["title"] = "Course grading — Group 15 (local)"
    dashboard["refresh"] = "5s"
    dashboard["time"] = {"from": "now-15m", "to": "now"}
    dashboard["description"] = (
        "Original instructor Group View queries against local instructor validators. "
        "A pass requires nonzero required notifications and HTTP validations. "
        "Run python -m loadtests.course check for an explicit result; no data is not a pass."
    )
    for var in dashboard["templating"]["list"]:
        value = "group15" if var["name"] == "group" else "course-test-events"
        var["query"] = value
        var["current"] = {"selected": True, "text": value, "value": value}
        var["options"] = [{"selected": True, "text": value, "value": value}]
    bottom = max(panel["gridPos"]["y"] + panel["gridPos"]["h"] for panel in dashboard["panels"])
    dashboard["panels"].append(
        {
            "id": 1000,
            "type": "stat",
            "title": "Latest local test",
            "description": "Additional runner check: nonzero real load, both HTTP endpoints correct, "
            "throughput >=95% of target, and all required notifications received in <10s. "
            "The instructor panels above retain their original queries.",
            "gridPos": {"x": 0, "y": bottom, "w": 24, "h": 4},
            "datasource": {"type": "prometheus", "uid": "course-prometheus"},
            "targets": [{"refId": "A", "expr": "course_test_passed", "instant": True}],
            "fieldConfig": {
                "defaults": {
                    "noValue": "NO TEST DATA",
                    "mappings": [
                        {
                            "type": "value",
                            "options": {
                                "-1": {"text": "FAIL", "color": "red"},
                                "0": {"text": "NOT COMPLETED", "color": "yellow"},
                                "1": {"text": "PASS", "color": "green"},
                            },
                        }
                    ],
                    "thresholds": {
                        "mode": "absolute",
                        "steps": [
                            {"color": "red", "value": None},
                            {"color": "yellow", "value": 0},
                            {"color": "green", "value": 1},
                        ],
                    },
                },
                "overrides": [],
            },
            "options": {
                "colorMode": "background",
                "graphMode": "none",
                "reduceOptions": {"calcs": ["lastNotNull"], "values": False},
            },
        },
    )
    write(STATE / "dashboards/group15.json", json.dumps(dashboard, indent=2) + "\n")
    if not (STATE / "metrics/run.prom").exists():
        publish_result(0)
    print(
        "Prepared local certificates, test JWT, workload and original grading dashboard.",
        flush=True,
    )


def publish_result(status):
    path = STATE / "metrics/run.prom"
    temporary = path.with_suffix(".tmp")
    write(
        temporary,
        "# HELP course_test_passed Latest local instructor run: -1 fail, 0 incomplete, 1 pass.\n"
        "# TYPE course_test_passed gauge\n" + f"course_test_passed {status}\n",
    )
    temporary.replace(path)


class Docker:
    def __init__(self, command):
        self.command = shlex.split(command)
        if (
            command == "docker"
            and subprocess.run(
                self.command + ["info"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL
            ).returncode
        ):
            self.command = ["sudo", "-n", "docker"]
        self.compose = self.command + ["compose"]
        if (ROOT / ".env.monitoring").exists():
            self.compose += ["--env-file", str(ROOT / ".env.monitoring")]
        self.compose += [
            "--env-file",
            str(STATE / "run.env"),
            "-f",
            "compose.yml",
            "-f",
            "compose.local.yml",
            "-f",
            "compose.course-test.yml",
        ]
        self.containers = {}

    def run(self, *args, capture=False):
        if args[0] in {"up", "rm", "down"}:
            self.containers.clear()
        return subprocess.run(
            self.compose + list(args), cwd=ROOT, check=True, text=True, capture_output=capture
        ).stdout

    def state(self, service):
        return self.states(service)[service]

    def states(self, *services):
        # Resolving Compose's entire graph twice per poll was itself a material
        # CPU load on the shared 2-vCPU test VM. Resolve IDs once and inspect all
        # requested containers in one cheap daemon call. Mutations invalidate IDs.
        result, present = {}, []
        for service in services:
            if service not in self.containers:
                self.containers[service] = self.run("ps", "-a", "-q", service, capture=True).strip()
            if self.containers[service]:
                present.append(service)
            else:
                result[service] = {"Status": "missing", "ExitCode": -1}
        if present:
            raw = subprocess.check_output(
                self.command
                + ["inspect", "--format", "{{json .State}}"]
                + [self.containers[service] for service in present],
                text=True,
            )
            result.update(zip(present, map(json.loads, raw.splitlines()), strict=True))
        return result


def get(url):
    with urllib.request.urlopen(url, timeout=5) as response:
        return response.read().decode()


def wait_http(url, timeout=60):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            return get(url)
        except (OSError, urllib.error.URLError):
            time.sleep(1)
    raise RuntimeError(f"Service did not become ready: {url}")


def metrics(url):
    return [
        sample for family in text_string_to_metric_families(get(url)) for sample in family.samples
    ]


def http_summary():
    samples = metrics("http://127.0.0.1:3002/metrics")
    counts = {}
    rates = {}
    for sample in samples:
        if sample.labels.get("host_name") != "group15":
            continue
        if sample.name == "http_load_generator_response_count_total":
            key = sample.labels["endpoint"] + ":" + sample.labels["response_type"]
            counts[key] = int(sample.value)
        if sample.name.endswith("_request_rate"):
            rates[sample.name] = sample.value
    return counts, rates


def notification_summary(docker, since):
    sql = f"""
        SELECT json_build_object(
          'required', COUNT(*),
          'received', COUNT(n.measurement_id),
          'missing', COUNT(*) FILTER (WHERE n.measurement_id IS NULL),
          'at_or_over_10_seconds', COUNT(*) FILTER (WHERE n.latency >= 10),
          'max_latency_seconds', MAX(n.latency)
        )
        FROM demo.notification_ground_truth gt
        LEFT JOIN demo.notification n ON n.experiment_id=gt.experiment_id
          AND n.measurement_id=gt.measurement_id AND n.group_id='group15'
        WHERE gt.insert_timestamp >= to_timestamp({float(since)}) AT TIME ZONE 'UTC';
    """
    return json.loads(
        docker.run(
            "exec",
            "-T",
            "course-db",
            "psql",
            "-U",
            "course",
            "-d",
            "course",
            "-At",
            "-c",
            sql,
            capture=True,
        )
    )


def document_consumer():
    return Consumer(
        {
            "bootstrap.servers": "localhost:19092",
            "group.id": "course-document-check",
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "allow.auto.create.topics": False,
        }
    )


def document_offsets(consumer):
    topic = "course-test-documents"
    metadata = consumer.list_topics(topic, timeout=10)
    return {
        str(p): consumer.get_watermark_offsets(TopicPartition(topic, p), timeout=5)[1]
        for p in metadata.topics[topic].partitions
    }


def validate_histories(result):
    consumer = document_consumer()
    documents = []
    try:
        ends = document_offsets(consumer)
        pending = {int(p): end for p, end in ends.items() if end > result["document_offsets"][p]}
        consumer.assign(
            [
                TopicPartition("course-test-documents", p, result["document_offsets"][str(p)])
                for p in pending
            ]
        )
        deadline = time.monotonic() + 15
        while pending and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None:
                continue
            if message.error():
                raise RuntimeError(str(message.error()))
            if message.partition() not in pending:
                continue
            documents.extend(fastavro.reader(io.BytesIO(message.value())))
            if message.offset() + 1 >= pending[message.partition()]:
                del pending[message.partition()]
    finally:
        consumer.close()
    failures = []
    for doc in documents:
        expected = sorted(doc["measurements"], key=lambda m: m["timestamp"])
        limits = doc["temperature_range"]
        outside = [
            m
            for m in expected
            if m["temperature"] < limits["lower_threshold"]
            or m["temperature"] > limits["upper_threshold"]
        ]
        query = urllib.parse.urlencode(
            {
                "experiment-id": doc["experiment"],
                "start-time": 0,
                "end-time": max(m["timestamp"] for m in expected) + 1,
            }
        )
        for endpoint, wanted in (
            ("/temperature", expected),
            ("/temperature/out-of-range", outside),
        ):
            try:
                actual = json.loads(get("http://127.0.0.1:3003" + endpoint + "?" + query))
                actual.sort(key=lambda m: m["timestamp"])
                # Same 0.001 tolerances as the instructor's Measurement::eq.
                correct = len(actual) == len(wanted) and all(
                    abs(a[field] - b[field]) < 0.001
                    for a, b in zip(actual, wanted, strict=True)
                    for field in ("timestamp", "temperature")
                )
                if not correct:
                    failures.append({"experiment": doc["experiment"], "endpoint": endpoint})
            except (OSError, ValueError, TypeError, KeyError) as exc:
                failures.append(
                    {"experiment": doc["experiment"], "endpoint": endpoint, "error": str(exc)}
                )
    expected_count = result["concurrent_experiments"] + result["warmup_experiments"]
    unique_count = len({d["experiment"] for d in documents})
    return {
        "expected_experiments": expected_count,
        "completed_experiments": unique_count,
        "history_endpoint_checks": len(documents) * 2,
        "failures": failures,
        "passed": not pending and unique_count == expected_count and not failures,
    }


def check(docker, result):
    histories = validate_histories(result)
    counts, rates = http_summary()
    notifications = notification_summary(docker, result["started"])
    notification_errors = sum(
        s.value
        for s in metrics("http://127.0.0.1:3000/api/metrics")
        if s.name == "notifications_service_response_count_total"
        and s.labels.get("response_type") != "Ok"
    )
    http_errors = sum(count for key, count in counts.items() if not key.endswith(":Ok"))
    http_ok = sum(count for key, count in counts.items() if key.endswith(":Ok"))
    observation = max(0, time.time() - result.get("first_http_timestamp", time.time()))
    validated = max(0, http_ok - result.get("initial_http_ok", 0))
    achieved_rps = validated / observation if observation else 0
    enough_load = (
        observation >= result["http_seconds"] and achieved_rps >= result["http_rps"] * 0.95
    )
    variable = None
    if result.get("http_max_rps"):
        variable = variable_http_phases(result.get("http_load_samples", []))
        enough_load = enough_load and variable["passed"]
    successful_endpoints = all(
        counts.get(endpoint + ":Ok", 0) > 0
        for endpoint in (
            "/temperature",
            "/temperature/out-of-bounds",
        )
    )
    producer = docker.state("course-producer")
    passed = (
        producer["Status"] == "exited"
        and producer["ExitCode"] == 0
        and notifications["required"] > 0
        and notifications["missing"] == 0
        and notifications["at_or_over_10_seconds"] == 0
        and meets_notification_deadline(notifications["max_latency_seconds"])
        and notification_errors == 0
        and http_errors == 0
        and successful_endpoints
        and enough_load
        and histories["passed"]
    )
    status = 1 if passed else -1
    publish_result(status)
    observed = False
    for _ in range(8):
        samples = json.loads(get("http://127.0.0.1:3005/api/v1/query?query=course_test_passed"))[
            "data"
        ]["result"]
        if samples and all(float(sample["value"][1]) == status for sample in samples):
            observed = True
            break
        time.sleep(1)
    result.update(
        {
            "finished": time.time(),
            "passed": passed,
            "panel_status_observed": observed,
            "notifications": notifications,
            "histories": histories,
            "notification_errors": notification_errors,
            "http_responses": counts,
            "http_rates": rates,
            "http_observation_seconds": observation,
            "achieved_http_rps": achieved_rps,
            "enough_http_load": enough_load,
            "variable_http_validation": variable,
            "producer_exit_code": producer["ExitCode"],
            "producer_status": producer["Status"],
        }
    )
    result["dashboard"] = (
        "http://localhost:3009/d/cec-course-grading/course-grading"
        f"?from={int(result['started'] * 1000)}&to={int(result['finished'] * 1000)}"
        "&var-group=group15&var-topic=course-test-events"
    )
    # Opening the panel later should show the completed test, not an empty
    # rolling window. A new run restores the live window in prepare().
    dashboard_path = STATE / "dashboards/group15.json"
    dashboard = json.loads(dashboard_path.read_text())
    dashboard["time"] = {
        bound: datetime.fromtimestamp(result[field], UTC).isoformat()
        for bound, field in (("from", "started"), ("to", "finished"))
    }
    write(dashboard_path, json.dumps(dashboard, indent=2) + "\n")
    write(LATEST, json.dumps(result, indent=2) + "\n")
    write(RESULTS / f"course-{int(result['started'])}.json", json.dumps(result, indent=2) + "\n")
    print(json.dumps(result, indent=2), flush=True)
    return passed


def course_tools_fingerprint(root=ROOT):
    # Match the public source inputs of CourseTools.Dockerfile. Never hash tokens,
    # auth directories, .env files, target/, or unrelated Python application code.
    paths = {
        root / name
        for name in (
            "Cargo.toml",
            "Cargo.lock",
            ".dockerignore",
            "deploy/CourseTools.Dockerfile",
            "deploy/CourseTools.Dockerfile.dockerignore",
            "notifications-service/ca.crt",
        )
    }
    workspace = tomllib.loads((root / "Cargo.toml").read_text())["workspace"]["members"]
    for pattern in workspace:
        for member in root.glob(pattern):
            paths.add(member / "Cargo.toml")
            paths.update(p for p in (member / "src").rglob("*") if p.is_file())
    for directory in (root / ".sqlx", root / "experiment-producer/schemas"):
        paths.update(p for p in directory.rglob("*") if p.is_file())
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(
            path.relative_to(root).as_posix().encode() + b"\0" + path.read_bytes() + b"\0"
        )
    return digest.hexdigest()


def ensure_course_tools(docker, force=False):
    fingerprint = course_tools_fingerprint()
    record = STATE / "tool-build.json"
    known = json.loads(record.read_text()) if record.exists() else {}
    image = subprocess.run(
        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
        capture_output=True,
        text=True,
    )
    current = image.stdout.strip() if image.returncode == 0 else None
    if not force and current and known == {"source_sha256": fingerprint, "image_id": current}:
        print("Reusing instructor tools: source fingerprint and image ID match.", flush=True)
        return False
    # Use one CLI builder consistently. A Rust rebuild can need several GiB;
    # matching completed binaries must not require retaining a huge builder image.
    subprocess.run(
        docker.command
        + ["build", "-f", "deploy/CourseTools.Dockerfile", "-t", "cec-course-tools:local", "."],
        cwd=ROOT,
        check=True,
    )
    current = subprocess.check_output(
        docker.command + ["image", "inspect", "--format", "{{.Id}}", "cec-course-tools:local"],
        text=True,
    ).strip()
    write(record, json.dumps({"source_sha256": fingerprint, "image_id": current}, indent=2) + "\n")
    return True


def start(docker, args):
    if not args.skip_build:
        ensure_course_tools(docker, force=args.rebuild_tools)
        docker.run("build", "data", "api")
    docker.run("up", "-d", "--wait", "kafka", "course-db")
    for topic in ("course-test-events", "course-test-documents"):
        docker.run(
            "exec",
            "-T",
            "kafka",
            "/opt/kafka/bin/kafka-topics.sh",
            "--bootstrap-server",
            "kafka:9092",
            "--create",
            "--if-not-exists",
            "--topic",
            topic,
            "--partitions",
            "16",
            "--replication-factor",
            "1",
        )
    docker.run(
        "up",
        "-d",
        "--wait",
        "data",
        "executor",
        "api",
        "scheduler",
        "notification",
        "course-notifications",
        "course-prometheus",
        "course-grafana",
        "node-exporter",
    )
    wait_http("http://127.0.0.1:3000/api/metrics")
    wait_http("http://127.0.0.1:3009/api/health")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "action", choices=("prepare", "up", "run", "check", "stop"), nargs="?", default="run"
    )
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--experiments", type=int, default=3)
    parser.add_argument("--sensors", type=int, default=3)
    parser.add_argument("--samples", type=int, default=20)
    parser.add_argument("--http-rps", type=int, default=100)
    parser.add_argument(
        "--http-max-rps", type=int, help="Inclusive upper rate for original Rust random generations"
    )
    parser.add_argument("--http-phase-seconds", type=int, default=20)
    parser.add_argument("--http-seconds", type=int, default=60)
    parser.add_argument("--skip-build", action="store_true")
    parser.add_argument(
        "--rebuild-tools",
        action="store_true",
        help="Recompile the instructor tools even if their source/image match",
    )
    args = parser.parse_args()
    if args.skip_build and args.rebuild_tools:
        parser.error("--skip-build and --rebuild-tools are mutually exclusive")
    if min(args.experiments, args.sensors, args.samples, args.http_rps, args.http_seconds) < 1:
        parser.error("Workload sizes must be positive")
    if args.http_rps > 65534 or args.samples > 65535:
        parser.error("Workload exceeds instructor CLI integer limits")
    if not 10 <= args.http_phase_seconds <= 65535:
        parser.error("HTTP phases must last 10..65535 seconds")
    if args.http_max_rps is not None:
        if not args.http_rps < args.http_max_rps <= 65534:
            parser.error("Maximum HTTP rate must exceed the minimum and be <=65534")
        if args.http_seconds < args.http_phase_seconds * 3:
            parser.error("Variable validation needs at least three HTTP generations")
    os.chdir(ROOT)
    if args.action in ("prepare", "up", "run"):
        prepare(args)
    if args.action == "prepare":
        return
    docker = Docker(args.docker_command)
    if args.action == "stop":
        docker.run("stop", "course-http", "course-producer")
        return
    if args.action == "check":
        if not LATEST.exists():
            raise SystemExit("No completed course test exists. Run the test first.")
        raise SystemExit(0 if check(docker, json.loads(LATEST.read_text())) else 1)
    start(docker, args)
    if args.action == "up":
        print("Grading dashboard: http://localhost:3009/d/cec-course-grading")
        return
    docker.run("stop", "course-http", "course-producer")
    # Reset validator counters for this run, preserving the dedicated results DB.
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-notifications")
    wait_http("http://127.0.0.1:3000/api/metrics")
    result = {
        "started": time.time(),
        "passed": False,
        "concurrent_experiments": args.experiments,
        "warmup_experiments": 1,
        "sensors": args.sensors,
        "sample_period_seconds": 1,
        "carry_out_samples": args.samples,
        "http_rps": args.http_rps,
        "http_max_rps": args.http_max_rps,
        "http_phase_seconds": args.http_phase_seconds,
        "http_load_samples": [],
        "http_seconds": args.http_seconds,
    }
    consumer = document_consumer()
    try:
        result["document_offsets"] = document_offsets(consumer)
    finally:
        consumer.close()
    write(LATEST, json.dumps(result, indent=2) + "\n")
    publish_result(0)
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-http")
    wait_http("http://127.0.0.1:3002/metrics")
    docker.run("up", "-d", "--no-deps", "--force-recreate", "course-producer")
    print(
        "Instructor workload running; dashboard: http://localhost:3009/d/cec-course-grading",
        flush=True,
    )
    deadline = time.monotonic() + 15 + 10 + args.samples + args.http_seconds + 90
    first_http = None
    while time.monotonic() < deadline:
        states = docker.states("course-http", "course-producer")
        if states["course-http"]["Status"] != "running":
            raise RuntimeError("Instructor HTTP validator exited; inspect its container logs")
        counts, rates = http_summary()
        if counts and first_http is None:
            first_http = time.monotonic()
            result["first_http_timestamp"] = time.time()
            result["initial_http_ok"] = sum(
                count for key, count in counts.items() if key.endswith(":Ok")
            )
            write(LATEST, json.dumps(result, indent=2) + "\n")
        if first_http is not None:
            result["http_load_samples"].append(
                {
                    "elapsed": time.monotonic() - first_http,
                    "target_rps": rates.get("http_load_generator_target_request_rate", 0),
                    "effective_rps": rates.get("http_load_generator_effective_request_rate", 0),
                    "http_ok": sum(count for key, count in counts.items() if key.endswith(":Ok")),
                }
            )
        state = states["course-producer"]
        if state["Status"] == "exited":
            if state["ExitCode"] != 0:
                raise RuntimeError("Instructor producer failed; inspect its container logs")
            if first_http is not None and time.monotonic() - first_http >= args.http_seconds:
                break
        time.sleep(1)
    else:
        raise RuntimeError(
            "Timed out waiting for a completed producer and real HTTP validation traffic"
        )
    result["http_observation_seconds"] = time.monotonic() - first_http
    write(
        RESULTS / f"course-{int(result['started'])}-producer.log",
        docker.run("logs", "--no-color", "course-producer", capture=True),
    )
    raise SystemExit(0 if check(docker, result) else 1)


if __name__ == "__main__":
    main()
````

## loadtests/http\_profile.py

Independent intended-send scheduling with bounded pending work, omitted-offer detection and phase drain accounting.

SHA256: `22c7983708d3e3af991b6e832ecd729d24811a23a8e6f956530d897ed0bf9fcc`

````python
"""Open-loop HTTP load with separate accounting for every rate change.

Latency starts at the intended send time, including generator and semaphore
delays. Completion after a phase extends that phase's denominator; draining a
backlog in a later, easier phase cannot turn an overloaded phase into a pass.
"""

import argparse
import asyncio
import math
import time
from dataclasses import dataclass, field

from .workload import distribution


@dataclass(frozen=True)
class Phase:
    rate: int
    seconds: float

    @property
    def requests(self):
        return math.ceil(self.rate * self.seconds - 1e-9)


def parse_profile(value):
    try:
        result = [
            Phase(int(rate), float(seconds))
            for rate, seconds in (part.split(":") for part in value.split(","))
        ]
        if any(p.rate < 0 or not math.isfinite(p.seconds) or p.seconds <= 0 for p in result):
            raise ValueError
        if not any(p.rate for p in result):
            raise ValueError
        return result
    except (ValueError, TypeError) as exc:
        raise argparse.ArgumentTypeError(
            "profile must be RATE:SECONDS phases, e.g. 50:20,200:30,100:20; "
            "durations positive, rates nonnegative, at least one rate positive"
        ) from exc


@dataclass
class PhaseResult:
    phase: Phase
    start: float
    scheduled: int = 0
    succeeded: int = 0
    errors: int = 0
    last_completed: float = 0
    latencies: list[float] = field(default_factory=list)
    scheduling_delays: list[float] = field(default_factory=list)
    unexpected_errors: list[str] = field(default_factory=list)

    def report(self):
        elapsed = max(self.phase.seconds, self.last_completed - self.start)
        rate = self.succeeded / elapsed
        return {
            "target_rps": self.phase.rate,
            "duration_seconds": self.phase.seconds,
            "expected_requests": self.phase.requests,
            "scheduled_requests": self.scheduled,
            "successful_requests": self.succeeded,
            "errors": self.errors,
            "unexpected_error_types": sorted(set(self.unexpected_errors)),
            "elapsed_including_drain_seconds": elapsed,
            "achieved_rps": rate,
            "latency_seconds": distribution(self.latencies),
            "scheduling_delay_seconds": distribution(self.scheduling_delays),
            "passed": self.scheduled == self.phase.requests
            and self.succeeded == self.scheduled
            and self.errors == 0
            and rate >= 0.95 * self.phase.rate,
        }


async def run_profile(phases, query, start, max_pending=1000):
    """query(index) returns success; bound tasks without hiding unsent requests."""
    pending, results = set(), []
    index = 0

    async def request(index, planned, result):
        result.scheduling_delays.append(max(0, time.monotonic() - planned))
        try:
            success = await query(index)
            result.succeeded += int(success)
            result.errors += int(not success)
        except Exception as exc:
            result.errors += 1
            result.unexpected_errors.append(type(exc).__name__)
        finally:
            now = time.monotonic()
            result.last_completed = max(result.last_completed, now)
            result.latencies.append(max(0, now - planned))

    try:
        for phase in phases:
            result = PhaseResult(phase, start)
            results.append(result)
            for offset in range(phase.requests):
                planned = start + offset / phase.rate
                await asyncio.sleep(max(0, planned - time.monotonic()))
                if len(pending) >= max_pending:
                    # Refuse to exhaust the generator. The expected/scheduled
                    # mismatch makes this run fail instead of silently dropping load.
                    index += 1
                    continue
                task = asyncio.create_task(request(index, planned, result))
                pending.add(task)
                task.add_done_callback(pending.discard)
                result.scheduled += 1
                index += 1
            start += phase.seconds
            await asyncio.sleep(max(0, start - time.monotonic()))
        if pending:
            await asyncio.gather(*pending)
    finally:
        for task in pending:
            task.cancel()
        await asyncio.gather(*pending, return_exceptions=True)
    return results
````

## loadtests/matrix.py

Repeat deterministic profiles across executor/polling settings and save sizing evidence.

SHA256: `177d96b2db63fbdd84bb786c746efcc2c7cca0931508045268269491f957fd4f`

````python
"""Measure executor and notification interval choices on the actual Docker host."""

import argparse
import asyncio
import json
import os
import shlex
import subprocess
import time
from pathlib import Path

import httpx

from .stress import parser as stress_parser
from .stress import run


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-command", default="docker")
    parser.add_argument("--replicas", default="1,2,4")
    parser.add_argument("--intervals", default="0.1,0.5,1")
    parser.add_argument("--duration", type=float, default=30)
    parser.add_argument("--sensors", type=int, default=10)
    parser.add_argument("--http-rps", type=int, default=200)
    parser.add_argument("--output-dir", default="test-results/matrix")
    args = parser.parse_args()
    command = [
        *shlex.split(args.docker_command),
        "compose",
        "-f",
        "compose.yml",
        "-f",
        "compose.local.yml",
    ]
    replicas = [int(n) for n in args.replicas.split(",")]
    intervals = [float(n) for n in args.intervals.split(",")]
    reports = []
    for count, interval in [(n, intervals[0]) for n in replicas] + [
        (replicas[0], t) for t in intervals[1:]
    ]:
        env = {**os.environ, "TO_NOTIFICATION_INTERVAL": str(interval)}
        await asyncio.to_thread(
            subprocess.run,
            [
                *command,
                "up",
                "-d",
                "--no-deps",
                "--no-build",
                "--scale",
                f"executor={count}",
                "executor",
                "notification",
            ],
            env=env,
            check=True,
            timeout=60,
        )
        async with httpx.AsyncClient(trust_env=False) as client:
            deadline = time.monotonic() + 60
            while True:
                response = await client.get("http://localhost:8001/status")
                if response.json()["executors"] == count:
                    break
                if time.monotonic() >= deadline:
                    raise RuntimeError("executor discovery did not converge")
                await asyncio.sleep(0.5)
        workload = stress_parser().parse_args(
            [
                "--duration",
                str(args.duration),
                "--sensors",
                str(args.sensors),
                "--http-rps",
                str(args.http_rps),
                "--output",
                f"{args.output_dir}/workers-{count}-interval-{interval}.json",
            ]
        )
        report = await run(workload)
        reports.append({"replicas": count, "notification_interval": interval, **report})
    path = Path(args.output_dir) / "summary.json"
    await asyncio.to_thread(path.write_text, json.dumps(reports, indent=2) + "\n")
    # Restore the measured lowest passing size and fastest tested interval.
    passing = [r for r in reports if r["passed"] and r["notification_interval"] == intervals[0]]
    selected = min((r["replicas"] for r in passing), default=replicas[0])
    await asyncio.to_thread(
        subprocess.run,
        [
            *command,
            "up",
            "-d",
            "--no-deps",
            "--no-build",
            "--scale",
            f"executor={selected}",
            "executor",
            "notification",
        ],
        env={**os.environ, "TO_NOTIFICATION_INTERVAL": str(intervals[0])},
        check=True,
        timeout=60,
    )
    if not passing:
        raise SystemExit(
            f"No tested executor count met the acceptance criteria. Restored {selected}; results: {path}"
        )
    print(f"Selected {selected} executor(s); poll interval {intervals[0]}s. Results: {path}")


if __name__ == "__main__":
    asyncio.run(main())
````

## loadtests/notification\_sink.py

Local independent-test receiver and fault injection; distinct from the original cryptographic validator.

SHA256: `4cbbc293269fdc89de6589541a30b3d35438575aab65f970283f8405ebe521c9`

````python
"""Test double only. Never use this service as the real Notifications API."""

import asyncio
import base64
import json
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from temperature_observability.models import Notification

app = FastAPI(title="Local notification contract validator")
received: dict[tuple, dict] = {}
faults = {"remaining": 0, "status": 503, "delay": 0.0}


class Faults(BaseModel):
    remaining: int = Field(default=0, ge=0)
    status: int = Field(default=503, ge=400, le=599)
    delay: float = Field(default=0, ge=0, le=10)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/api/notify")
async def notify(body: Notification):
    if faults["delay"]:
        await asyncio.sleep(faults["delay"])
    if faults["remaining"]:
        faults["remaining"] -= 1
        raise HTTPException(faults["status"], "injected test failure")
    try:
        truth = json.loads(base64.b64decode(body.cipher_data, validate=True))
    except (ValueError, TypeError) as exc:
        raise HTTPException(400, "expected local test ciphertext") from exc
    for field in ("notification_type", "researcher", "experiment_id", "measurement_id"):
        if truth.get(field) != getattr(body, field):
            raise HTTPException(400, f"incorrect {field}")
    identity = (body.experiment_id, body.measurement_id, body.notification_type)
    if identity not in received:
        received[identity] = {
            **body.model_dump(),
            "latency": time.time() - truth["timestamp"],
            "attempts": 0,
        }
    received[identity]["attempts"] += 1
    return received[identity]["latency"]


@app.get("/test/notifications")
async def notifications(prefix: str = ""):
    return [r for r in received.values() if r["experiment_id"].startswith(prefix)]


@app.post("/test/faults")
async def set_faults(config: Faults):
    faults.update(config.model_dump())
    return faults
````

## loadtests/recovery.py

Exercise persisted partial groups, logical replay, delivery failures, component restarts and cold-cache SQL recovery.

SHA256: `0154ce146a9ac7063521f71df74b513680da04b3b267a31bf38c5f6f1e596809`

````python
"""Exercise durable partial groups, worker loss, consumer replay and Redis failure."""

import argparse
import asyncio
import json
import shlex
import subprocess
import time
from uuid import uuid4

import httpx

from .stress import wait_for_experiments
from .workload import Experiment, LocalProducer


async def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--docker-command", default="docker")
    args = parser.parse_args()
    compose = [
        *shlex.split(args.docker_command),
        "compose",
        "-f",
        "compose.yml",
        "-f",
        "compose.local.yml",
    ]

    async def docker(*arguments):
        await asyncio.to_thread(
            subprocess.run,
            [*compose, *arguments],
            check=True,
            timeout=60,
            stdout=subprocess.DEVNULL,
        )

    async with httpx.AsyncClient(timeout=5, trust_env=False) as client:
        producer = LocalProducer()
        experiment = Experiment(producer, "recovery-" + uuid4().hex, sensors=2)
        experiment.configure()
        experiment.transition("stabilization_started")
        experiment.measure(25.5)
        experiment.transition("experiment_started")
        await producer.flush()
        await wait_for_experiments(client, "http://localhost:3003", [experiment], "running")
        # Save only one sensor to the real data service, then replace the executor.
        experiment.measure(27)
        # Both readings above are already queued; the next group is deliberately split.
        await producer.flush()
        saved = experiment.last_events[0].copy()
        saved.update(measurement_id=str(uuid4()), timestamp=time.time(), temperature=27)
        # Use an unchanged out-of-range sample, so this new group should not alert.
        producer.send("sensor_temperature_measured", saved)
        await producer.flush()
        for _ in range(100):
            state = (await client.get(f"http://localhost:3003/experiments/{experiment.id}")).json()
            if state["pending_measurements"] == 1:
                break
            await asyncio.sleep(0.1)
        else:
            raise AssertionError("partial group was not persisted")
        await docker("restart", "executor")
        other = dict(saved, sensor=next(s for s in experiment.sensors if s != saved["sensor"]))
        producer.send("sensor_temperature_measured", other)
        experiment.history.append({"timestamp": saved["timestamp"], "temperature": 27})
        await producer.flush()
        # Force a notification retry, then create another excursion.
        response = await client.post(
            "http://localhost:18080/test/faults", json={"remaining": 2, "status": 503}
        )
        response.raise_for_status()
        experiment.measure(25.5)
        experiment.measure(24)
        for event in experiment.last_events:  # Replay the same logical group at NEW offsets.
            producer.send("sensor_temperature_measured", event)
        experiment.transition("experiment_terminated")
        await producer.flush()
        await docker("restart", "scheduler")
        await wait_for_experiments(client, "http://localhost:3003", [experiment], "terminated")
        params = {"experiment-id": experiment.id, "start-time": 0, "end-time": time.time()}
        for _ in range(150):
            notifications = (
                await client.get(
                    "http://localhost:18080/test/notifications", params={"prefix": experiment.id}
                )
            ).json()
            if len(notifications) == len(experiment.alerts):
                break
            await asyncio.sleep(0.1)
        assert {(n["measurement_id"], n["notification_type"]) for n in notifications} == set(
            experiment.alerts.items()
        )
        response = await client.get("http://localhost:3003/temperature", params=params)
        assert response.json() == experiment.history
        await docker("stop", "redis")
        try:
            response = await client.get("http://localhost:3003/temperature", params=params)
            assert response.status_code == 200 and response.json() == experiment.history
            for service in ("data", "postgres"):
                await docker("restart", service)
                if service == "postgres":
                    # Clear the new process-local read snapshots too: a cached
                    # response would not prove that PostgreSQL recovered its data.
                    await docker("restart", "data")
                for _ in range(150):
                    try:
                        response = await client.get(
                            "http://localhost:3003/temperature", params=params
                        )
                        if response.status_code == 200:
                            break
                    except httpx.HTTPError:
                        pass
                    await asyncio.sleep(0.1)
                assert response.status_code == 200 and response.json() == experiment.history
        finally:
            await docker("start", "redis")
            await client.post("http://localhost:18080/test/faults", json={"remaining": 0})
        print(
            json.dumps(
                {
                    "passed": True,
                    "experiment": experiment.id,
                    "history_points": len(experiment.history),
                    "notifications": len(notifications),
                    "checks": [
                        "partial sensor group survives executor restart",
                        "duplicate group at new offsets",
                        "notification retries after HTTP 503",
                        "scheduler restart",
                        "Redis fallback",
                        "data service restart preserves history",
                        "PostgreSQL restart preserves history with Redis stopped and Data cache cleared",
                    ],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    asyncio.run(main())
````

## loadtests/stress.py

Independent end-to-end workload driver, complete-history/alert validation, strict deadlines and phase/telemetry results.

SHA256: `6db15d49fed07c050d7f9cf4c0371312b8e36eb8a40e0a057c5e24e99f3696f7`

````python
"""End-to-end Avro/Kafka workload with independent history and notification checks."""

import argparse
import asyncio
import json
import math
import time
from collections import Counter
from pathlib import Path
from uuid import uuid4

import httpx

from temperature_observability.common import http_client

from .http_profile import Phase, parse_profile, run_profile
from .telemetry import Telemetry
from .validation import meets_notification_deadline
from .workload import Experiment, LocalProducer, distribution


async def wait_for_experiments(client, base_url, experiments, status, deadline_seconds=60):
    deadline = time.monotonic() + deadline_seconds
    pending = set(e.id for e in experiments)
    semaphore = asyncio.Semaphore(20)

    async def inspect(experiment):
        try:
            async with semaphore:
                return await client.get(base_url + f"/experiments/{experiment}")
        except httpx.HTTPError:
            return None

    while pending and time.monotonic() < deadline:
        batch = list(pending)
        responses = await asyncio.gather(*(inspect(experiment) for experiment in batch))
        for experiment, response in zip(batch, responses, strict=True):
            if (
                response is not None
                and response.status_code == 200
                and response.json()["status"] == status
            ):
                pending.remove(experiment)
        if pending:
            await asyncio.sleep(0.1)
    if pending:
        raise RuntimeError(
            f"{len(pending)} experiments did not reach {status} within {deadline_seconds}s"
        )


async def run(args):
    profile = getattr(args, "http_profile", None)
    phases = parse_profile(profile) if profile else [Phase(args.http_rps, args.duration)]
    duration = sum(phase.seconds for phase in phases)
    prefix = "stress-" + uuid4().hex[:12]
    producer = LocalProducer(args.brokers, args.topic)
    experiments = [
        Experiment(producer, f"{prefix}-{i}", args.sensors) for i in range(args.experiments)
    ]
    async with http_client() as client:
        ready_deadline = time.monotonic() + 60
        while True:
            try:
                response = await client.get(args.scheduler + "/health")
                if response.status_code == 200:
                    break
            except httpx.HTTPError:
                pass
            if time.monotonic() >= ready_deadline:
                raise RuntimeError("scheduler did not become ready before the workload")
            await asyncio.sleep(0.5)
        for experiment in experiments:
            experiment.configure()
            experiment.transition("stabilization_started")
            experiment.measure(20)
            experiment.measure(25.5)
            experiment.transition("experiment_started")
        await producer.flush()
        await wait_for_experiments(client, args.api, experiments, "running")
        http_errors = []
        queue_peak = 0
        telemetry = Telemetry(getattr(args, "data", "http://localhost:8002"))
        stop = asyncio.Event()
        semaphore = asyncio.Semaphore(50)

        async def query(index):
            experiment = experiments[index % len(experiments)]
            try:
                async with asyncio.timeout(10), semaphore:
                    path = "/temperature" if index % 2 else "/temperature/out-of-range"
                    params = {"experiment-id": experiment.id}
                    if path == "/temperature":
                        params.update({"start-time": 0, "end-time": time.time()})
                    response = await client.get(args.api + path, params=params)
                    response.raise_for_status()
                    points = response.json()
                    assert isinstance(points, list)
                    assert all(set(point) == {"timestamp", "temperature"} for point in points)
                return True
            except (httpx.HTTPError, ValueError, AssertionError, TimeoutError) as exc:
                http_errors.append(type(exc).__name__)
                return False

        async def watch_queue():
            nonlocal queue_peak
            while not stop.is_set():
                try:
                    response = await client.get(args.scheduler + "/status")
                    response.raise_for_status()
                    queue_peak = max(queue_peak, response.json()["queued_events"])
                    await telemetry.sample(client, start, response.json()["queued_events"])
                except httpx.HTTPError:
                    pass
                await asyncio.sleep(1)

        temperatures = (25.5, 27, 28, 26, 24, 24, 25, 25.5)
        start = time.monotonic()
        load_task = asyncio.create_task(run_profile(phases, query, start))
        queue_task = asyncio.create_task(watch_queue())
        samples = max(1, math.ceil(duration / args.period))
        for sample in range(samples):
            for i, experiment in enumerate(experiments):
                # Vary phases between experiments while retaining sustained excursions.
                experiment.measure(temperatures[(sample + i) % len(temperatures)])
            await producer.flush()
            await asyncio.sleep(max(0, start + (sample + 1) * args.period - time.monotonic()))
        for experiment in experiments:
            experiment.transition("experiment_terminated")
        await producer.flush()
        elapsed = time.monotonic() - start
        stop.set()
        phase_results = await load_task
        http_phases = [phase.report() for phase in phase_results]
        await queue_task
        http_elapsed = time.monotonic() - start
        http_requests = sum(phase["scheduled_requests"] for phase in http_phases)
        await wait_for_experiments(client, args.api, experiments, "terminated")

        mismatches = []

        async def validate(experiment):
            async with semaphore:
                response = await client.get(
                    args.api + "/temperature",
                    params={
                        "experiment-id": experiment.id,
                        "start-time": 0,
                        "end-time": time.time(),
                    },
                )
                response.raise_for_status()
                if response.json() != experiment.history:
                    mismatches.append(experiment.id + ": history")
                response = await client.get(
                    args.api + "/temperature/out-of-range", params={"experiment-id": experiment.id}
                )
                response.raise_for_status()
                truth = [
                    m for m in experiment.history if m["temperature"] < 25 or m["temperature"] > 26
                ]
                if response.json() != truth:
                    mismatches.append(experiment.id + ": out-of-range")
                point = experiment.history[len(experiment.history) // 2]
                response = await client.get(
                    args.api + "/temperature",
                    params={
                        "experiment-id": experiment.id,
                        "start-time": point["timestamp"],
                        "end-time": point["timestamp"],
                    },
                )
                if response.json() != [point]:
                    mismatches.append(experiment.id + ": inclusive interval")

        await asyncio.gather(*(validate(experiment) for experiment in experiments))
        expected_alerts = {
            (experiment.id, mid, kind)
            for experiment in experiments
            for mid, kind in experiment.alerts.items()
        }
        deadline = time.monotonic() + 30
        while True:
            response = await client.get(
                args.sink + "/test/notifications", params={"prefix": prefix}
            )
            response.raise_for_status()
            notifications = response.json()
            actual_alerts = {
                (n["experiment_id"], n["measurement_id"], n["notification_type"])
                for n in notifications
            }
            if expected_alerts <= actual_alerts or time.monotonic() >= deadline:
                break
            await asyncio.sleep(0.1)
        latency = distribution([n["latency"] for n in notifications])
        report = {
            "run_id": prefix,
            "configuration": vars(args),
            "generator_event_loop": type(asyncio.get_running_loop()).__module__,
            "duration_seconds": elapsed,
            "sensor_events_per_second": args.experiments * args.sensors / args.period,
            "actual_sensor_events_per_second": args.experiments * args.sensors * samples / elapsed,
            "actual_samples_per_second_per_experiment": samples / elapsed,
            "samples_per_experiment": samples,
            "kafka_messages": producer.sent,
            "http_requests_per_second": sum(p["successful_requests"] for p in http_phases)
            / http_elapsed,
            "http_attempts_per_second": http_requests / http_elapsed,
            "http_elapsed_seconds": http_elapsed,
            "http_phases": http_phases,
            "http_latency_seconds": distribution(
                [latency for phase in phase_results for latency in phase.latencies]
            ),
            "http_errors": sum(p["errors"] for p in http_phases),
            "http_error_types": dict(Counter(http_errors)),
            "notification_latency_seconds": latency,
            "notifications_expected": len(expected_alerts),
            "notifications_received": len(actual_alerts),
            "missing_notifications": len(expected_alerts - actual_alerts),
            "unexpected_notifications": len(actual_alerts - expected_alerts),
            "duplicate_delivery_attempts": sum(n["attempts"] - 1 for n in notifications),
            "data_mismatches": mismatches,
            "peak_scheduler_queue": queue_peak,
            "telemetry": telemetry.summary(),
        }
        report["passed"] = (
            not http_errors
            and not mismatches
            and expected_alerts == actual_alerts
            and all(meets_notification_deadline(n["latency"]) for n in notifications)
            and samples / elapsed >= 0.95 / args.period
            and all(phase["passed"] for phase in http_phases)
        )
        output = json.dumps(report, indent=2)
        print(output)
        if args.output:
            path = Path(args.output)
            await asyncio.to_thread(path.parent.mkdir, parents=True, exist_ok=True)
            await asyncio.to_thread(path.write_text, output + "\n")
            await asyncio.to_thread(
                path.with_suffix(".telemetry.json").write_text,
                json.dumps(telemetry.samples, indent=2) + "\n",
            )
        return report


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    result.add_argument("--experiments", type=int, default=100)
    result.add_argument("--sensors", type=int, default=10)
    result.add_argument("--duration", type=float, default=30)
    result.add_argument("--period", type=float, default=1)
    result.add_argument("--http-rps", type=int, default=200)
    result.add_argument(
        "--http-profile",
        help="RATE:SECONDS phases, e.g. 50:20,200:30,100:20; overrides duration/rps",
    )
    result.add_argument("--brokers", default="localhost:19092")
    result.add_argument("--topic", default="experiment")
    result.add_argument("--api", default="http://localhost:3003")
    result.add_argument("--sink", default="http://localhost:18080")
    result.add_argument("--scheduler", default="http://localhost:8001")
    result.add_argument(
        "--data", default="http://localhost:8002", help="Internal local Data URL for metrics"
    )
    result.add_argument("--output")
    return result


def main():
    args = parser().parse_args()
    if args.http_profile:
        parse_profile(args.http_profile)
    if (
        min(args.experiments, args.sensors, args.duration, args.period) <= 0
        or not all(math.isfinite(value) for value in (args.duration, args.period))
        or args.http_rps < 0
    ):
        raise SystemExit(
            "experiment count, sensors, duration, and period must be positive; HTTP rate nonnegative"
        )
    report = asyncio.run(run(args))
    raise SystemExit(0 if report["passed"] else 1)


if __name__ == "__main__":
    main()
````

## loadtests/telemetry.py

Sample host CPU/disk, queue and Data operation counters; diagnostics include cohosted test tools.

SHA256: `0e479b9ff92b896614843d3ca3def795bc98f8ea58952c61ea760a47e63f991d`

````python
"""Lightweight host and Data measurements alongside an end-to-end load test."""

import asyncio
import os
import time
from pathlib import Path

import httpx
from prometheus_client.parser import text_string_to_metric_families


class Telemetry:
    def __init__(self, data_url):
        self.data_url = data_url
        self.samples = []
        self.previous_cpu = None

    async def sample(self, client, start, queue):
        sample = {"elapsed_seconds": time.monotonic() - start, "queued_events": queue}
        # Linux host data describes the whole shared VM, including the load generator.
        try:
            cpu = await asyncio.to_thread(Path("/proc/stat").read_text)
            ticks = list(map(int, cpu.splitlines()[0].split()[1:9]))
            total, idle, steal = sum(ticks), ticks[3] + ticks[4], ticks[7]
            if self.previous_cpu:
                old_total, old_idle, old_steal = self.previous_cpu
                if total > old_total:
                    sample["host_cpu_utilization"] = 1 - (idle - old_idle) / (total - old_total)
                    sample["host_cpu_steal"] = (steal - old_steal) / (total - old_total)
            self.previous_cpu = total, idle, steal
            fs = os.statvfs("/")
            sample["disk_available_bytes"] = fs.f_bavail * fs.f_frsize
        except OSError:
            pass
        try:
            response = await client.get(self.data_url + "/metrics", timeout=2)
            response.raise_for_status()
            values = {}
            for family in text_string_to_metric_families(response.text):
                for metric in family.samples:
                    if (
                        metric.name.startswith("to_database_")
                        and not metric.name.endswith(("_bucket", "_created"))
                        or metric.name == "to_cache_reads_total"
                    ):
                        labels = ",".join(f"{k}={v}" for k, v in sorted(metric.labels.items()))
                        values[metric.name + ("{" + labels + "}" if labels else "")] = metric.value
            sample["data_metrics"] = values
        except (httpx.HTTPError, ValueError) as exc:
            sample["error"] = type(exc).__name__
        self.samples.append(sample)

    def summary(self):
        cpus = [s["host_cpu_utilization"] for s in self.samples if "host_cpu_utilization" in s]
        steals = [s["host_cpu_steal"] for s in self.samples if "host_cpu_steal" in s]
        measured = [s["data_metrics"] for s in self.samples if "data_metrics" in s]
        database = {}
        if len(measured) > 1:
            before, after = measured[0], measured[-1]
            for key, value in after.items():
                if "_seconds_sum{" in key and key in before:
                    count_key = key.replace("_sum{", "_count{")
                    count = after.get(count_key, 0) - before.get(count_key, 0)
                    if count > 0:
                        database[key.replace("_sum{", "_mean{")] = (value - before[key]) / count
        return {
            "host_cpu_mean": sum(cpus) / len(cpus) if cpus else None,
            "host_cpu_max": max(cpus, default=None),
            "host_cpu_steal_mean": sum(steals) / len(steals) if steals else None,
            "data_operation_mean_seconds": database,
            "samples": len(self.samples),
            "sample_errors": sum("error" in s for s in self.samples),
        }
````

## loadtests/validation.py

Strict below-ten deadline predicate and conservative settled native-variable phase validation.

SHA256: `5146bcf7fb01fd21d6b2f08c556b3e01bfaf0aadb5cddf20f8245228188e6e4d`

````python
"""Conservative pass criteria shared by the local workload runners."""

import math

NOTIFICATION_DEADLINE_SECONDS = 10.0


def meets_notification_deadline(latency):
    """Missing, invalid, and exactly-on-deadline observations are not passes."""
    return (
        latency is not None
        and math.isfinite(latency)
        and 0 <= latency < NOTIFICATION_DEADLINE_SECONDS
    )


def variable_http_phases(samples):
    """Check settled phases from the original Rust validator's public metrics.

    Its target gauge describes the batch being processed; its effective gauge
    describes the last completed batch. Exclude two seconds at transitions and
    require both the measured completion rate and every settled effective rate.
    This complements, rather than replaces, the independent open-loop test.
    """
    groups = []
    for sample in samples:
        if not groups or groups[-1][0]["target_rps"] != sample["target_rps"]:
            groups.append([])
        groups[-1].append(sample)
    phases = []
    for group in groups:
        target = group[0]["target_rps"]
        steady = [s for s in group if s["elapsed"] >= group[0]["elapsed"] + 2]
        if len(steady) < 2 or steady[-1]["elapsed"] - steady[0]["elapsed"] < 5:
            continue  # A truncated final phase is not evidence of a completed phase.
        elapsed = steady[-1]["elapsed"] - steady[0]["elapsed"]
        count = steady[-1]["http_ok"] - steady[0]["http_ok"]
        rate = count / elapsed
        minimum = min(s["effective_rps"] for s in steady)
        phases.append(
            {
                "target_rps": target,
                "observed_seconds": elapsed,
                "achieved_rps": rate,
                "minimum_effective_rps": minimum,
                "passed": target > 0 and rate >= target * 0.95 and minimum >= target * 0.95,
            }
        )
    return {
        "phases": phases,
        "distinct_rates": len({p["target_rps"] for p in phases}),
        "passed": len({p["target_rps"] for p in phases}) >= 2 and all(p["passed"] for p in phases),
    }
````

## loadtests/workload.py

Build deterministic sensor/event truth, serialize supplied Avro and calculate observed distributions.

SHA256: `18ad023166dd09d3a4281797e44bf4a328e883cab015a28f84af155dafe644ac`

````python
import asyncio
import base64
import io
import json
import math
import struct
import time
from pathlib import Path
from uuid import uuid4

import fastavro
from confluent_kafka import Producer


class LocalProducer:
    def __init__(self, brokers="localhost:19092", topic="experiment"):
        self.topic = topic
        self.client = Producer(
            {
                "bootstrap.servers": brokers,
                "enable.idempotence": True,
                "linger.ms": 5,
                "delivery.timeout.ms": 30000,
            }
        )
        directory = Path(__file__).resolve().parents[1] / "experiment-producer" / "schemas"
        self.schemas = {
            path.stem: fastavro.parse_schema(json.loads(path.read_text()))
            for path in directory.glob("*.avsc")
        }
        self.errors = []
        self.sent = 0

    def send(self, kind, payload):
        buffer = io.BytesIO()
        # Preserve the real producer's termination-container schema mismatch.
        schema_kind = "experiment_started" if kind == "experiment_terminated" else kind
        fastavro.writer(buffer, self.schemas[schema_kind], [payload])

        def delivery(error, _message):
            if error:
                self.errors.append(str(error))

        self.client.produce(
            self.topic,
            key=payload["experiment"],
            value=buffer.getvalue(),
            headers={"record_name": kind.encode()},
            on_delivery=delivery,
        )
        self.client.poll(0)
        self.sent += 1

    async def flush(self):
        remaining = await asyncio.to_thread(self.client.flush, 30)
        if remaining or self.errors:
            raise RuntimeError(
                f"Kafka delivery failed: {remaining} queued, {len(self.errors)} errors"
            )


class Experiment:
    def __init__(self, producer, experiment_id=None, sensors=2):
        self.producer = producer
        self.id = experiment_id or str(uuid4())
        self.sensors = [f"sensor-{i}" for i in range(sensors)]
        self.researcher = "test-researcher@example.invalid"
        self.phase = "configured"
        self.previous_outside = None
        self.history = []
        self.alerts = {}
        self.last_events = []

    def configure(self):
        self.producer.send(
            "experiment_configured",
            {
                "experiment": self.id,
                "researcher": self.researcher,
                "sensors": self.sensors,
                "temperature_range": {"lower_threshold": 25.0, "upper_threshold": 26.0},
            },
        )

    def transition(self, kind):
        self.producer.send(kind, {"experiment": self.id, "timestamp": time.time()})
        self.phase = {
            "stabilization_started": "stabilizing",
            "experiment_started": "running",
            "experiment_terminated": "terminated",
        }[kind]
        self.previous_outside = None

    def measure(self, average, *, timestamp=None):
        timestamp = time.time() if timestamp is None else timestamp
        measurement_id = str(uuid4())
        values = [
            struct.unpack("f", struct.pack("f", average + (i - (len(self.sensors) - 1) / 2) * 0.2))[
                0
            ]
            for i in range(len(self.sensors))
        ]
        actual_average = math.fsum(values) / len(values)
        outside = actual_average < 25 or actual_average > 26
        kind = None
        if self.phase == "stabilizing" and not outside and self.previous_outside is not False:
            kind = "Stabilized"
        if self.phase == "running" and outside and self.previous_outside is not True:
            kind = "OutOfRange"
        truth = {
            "experiment_id": self.id,
            "measurement_id": measurement_id,
            "researcher": self.researcher,
            "timestamp": timestamp,
            "notification_type": kind,
        }
        cipher = base64.b64encode(json.dumps(truth).encode()).decode()
        self.last_events = []
        for sensor, value in reversed(list(zip(self.sensors, values, strict=True))):
            event = {
                "experiment": self.id,
                "sensor": sensor,
                "temperature": value,
                "timestamp": timestamp,
                "measurement_id": measurement_id,
                "measurement_hash": cipher,
            }
            self.last_events.append(event)
            self.producer.send("sensor_temperature_measured", event)
        if self.phase == "running":
            self.history.append({"timestamp": timestamp, "temperature": actual_average})
        if kind:
            self.alerts[measurement_id] = kind
        self.previous_outside = outside


def percentile(values, fraction):
    if not values:
        return 0
    ordered = sorted(values)
    return ordered[min(len(ordered) - 1, math.ceil(len(ordered) * fraction) - 1)]


def distribution(values):
    return {
        "count": len(values),
        "p50": percentile(values, 0.5),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "max": max(values, default=0),
    }
````

## monitoring.env.example

Public tunable template, including allocated storage; real values live in an excluded private file.

SHA256: `44ea1de7c8779f0f3eed4ab80d94e21b0c94ae07ee4f1c81f0444b47f381f4cb`

````text
# Copy to .env.monitoring and pass --env-file .env.monitoring to Docker Compose.
TO_EXECUTOR_REPLICAS=1
TO_EXECUTOR_CAPACITY=8
TO_NOTIFICATION_INTERVAL=0.1
TO_NOTIFICATION_CONCURRENCY=32
# Overlays use different default groups so local and course checkpoints cannot mix.
# TO_KAFKA_GROUP=your-deployment-specific-group
TO_KAFKA_TOPIC=experiment
# Fill in the VM's actual billed hourly price and storage price (USD/GB/month).
TO_VM_HOURLY_COST_USD=0
TO_STORAGE_GB_MONTHLY_COST_USD=0
# Provisioned/billed disk capacity, not current database size (20 GiB on our VM).
TO_STORAGE_PROVISIONED_GB=0
# Unconfigured cost is shown as no data, never a claim of free infrastructure.
# This estimate excludes CPU surplus credits, public IPv4, traffic, tax and discounts.
# Internal PostgreSQL password. Use a URL-safe value when overriding.
TO_POSTGRES_PASSWORD=observability
````

## pyproject.toml

Python package/dependency/tool/test-marker configuration.

SHA256: `e10f86b33de22f35c77dbee2b8ccc716f7baef54ecf954879342fc041b3d626d`

````toml
[build-system]
requires = ["setuptools>=75"]
build-backend = "setuptools.build_meta"

[project]
name = "cec-temperature-observability"
version = "1.0.0"
description = "Durable, scalable temperature monitoring for XYZ Lab"
requires-python = ">=3.12"
dependencies = [
  "fastapi>=0.115,<1", "uvicorn[standard]>=0.34,<1", "asyncpg>=0.30,<1",
  "redis>=5,<7", "httpx>=0.28,<1", "confluent-kafka>=2.8,<3",
  "fastavro>=1.10,<2", "prometheus-client>=0.21,<1", "pydantic-settings>=2.8,<3",
  "sniffio>=1.3,<2",
  "aiohttp>=3.11,<4",
]

[project.optional-dependencies]
test = ["pytest>=8,<10", "pytest-asyncio>=0.25,<2", "ruff>=0.11,<1", "pyyaml>=6,<7"]

[tool.setuptools.packages.find]
include = ["temperature_observability*"]

[tool.setuptools.package-data]
temperature_observability = ["schema.sql"]

[tool.pytest.ini_options]
testpaths = ["tests"]
asyncio_mode = "auto"
markers = ["integration: requires the local Docker Compose stack"]

[tool.ruff]
line-length = 100
target-version = "py312"

[tool.ruff.lint]
select = ["E", "F", "I", "UP", "B", "ASYNC"]
ignore = ["E501", "B008"]
````

## requirements.lock

Pinned application dependency set for repeatable builds.

SHA256: `3fe93c4dc6b3b7fb256017fd499137ba17f4fdfc80f8e55013a009e927b4aa23`

````text
annotated-doc==0.0.5
annotated-types==0.8.0
anyio==4.15.1
asyncpg==0.32.0
certifi==2026.7.22
click==8.5.0
confluent-kafka==2.15.1
fastapi==0.142.2
fastavro==1.13.0
h11==0.16.0
httpcore==1.0.9
httptools==0.8.0
httpx==0.28.1
idna==3.20
iniconfig==2.3.1
opentelemetry-api==1.45.1
packaging==26.3
pluggy==1.6.0
prometheus_client==0.26.0
pydantic==2.13.5
pydantic-settings==2.15.0
pydantic_core==2.46.5
Pygments==2.21.0
pytest==9.1.1
pytest-asyncio==1.4.0
python-dotenv==1.2.4
PyYAML==6.0.3
redis==6.4.0
ruff==0.16.10
sniffio==1.3.1
starlette==1.7.0
typing-inspection==0.4.4
typing_extensions==4.16.0
uvicorn==0.54.0
uvloop==0.23.0
watchfiles==1.3.0
websockets==17.2
aiohappyeyeballs==2.7.1
aiohttp==3.14.4
aiosignal==1.4.0
attrs==26.1.0
frozenlist==1.8.0
multidict==6.9.1
propcache==0.5.4
yarl==1.25.1
````

## scripts/build.sh

Existing course repository build helper; inclusion does not claim new authorship.

SHA256: `056ee85939db0125d75643de476d3c3813e8eb73d21431e78e71132307b2a3e5`

````sh
#!/bin/bash

packages=(
    notifications-service 
    experiment-producer 
    http-load-generator 
    notifier 
    test-to-api
)

for package in ${packages[@]}; do
    echo "=== Building $package ==="
    docker build --build-arg="PACKAGE=${package}" -t "dclandau/demo-${package}" .
done

````

## scripts/check-course-kafka.py

Read-only external mTLS preflight using a separate temporary consumer group; never publishes or commits live offsets.

SHA256: `3a0dcbea083b166caf94053ae6882667c1f7e8f4412e73a5f2a46f9674820fa2`

````python
"""Read-only course Kafka metadata and consumer-assignment check; never publishes."""

import argparse
import json
import time
import uuid
from pathlib import Path

from confluent_kafka import Consumer
from confluent_kafka.admin import AdminClient

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("--credentials", type=Path, default=Path("group15-credentials"))
parser.add_argument(
    "--brokers",
    default="kafka.cec.dlandau.nl:19092,kafka.cec.dlandau.nl:29092,kafka.cec.dlandau.nl:39092",
)
parser.add_argument("--topic", default="experiment")
parser.add_argument("--strict-hostname", action="store_true")
parser.add_argument("--output", type=Path)
args = parser.parse_args()
mode = "https" if args.strict_hostname else "none"
result = {
    "timestamp": time.time(),
    "brokers": args.brokers,
    "topic": args.topic,
    "hostname_verification": mode,
    "connected": False,
}
consumer = None
try:
    config = {
        "bootstrap.servers": args.brokers,
        "security.protocol": "SSL",
        "ssl.ca.location": str(args.credentials / "ca.crt"),
        "ssl.keystore.location": str(args.credentials / "kafka.keystore.pkcs12"),
        "ssl.keystore.password": (args.credentials / "keystore-password").read_text().strip(),
        "ssl.endpoint.identification.algorithm": mode,
        "socket.timeout.ms": 5000,
        "log_level": 0,
    }
    metadata = AdminClient(config).list_topics(timeout=12)
    if args.topic not in metadata.topics or metadata.topics[args.topic].error:
        raise RuntimeError("Requested topic is not visible or has a metadata error")
    result["visible_topics"] = sorted(metadata.topics)
    consumer = Consumer(
        config
        | {
            "group.id": "group15-preflight-" + uuid.uuid4().hex[:12],
            "enable.auto.commit": False,
            "enable.auto.offset.store": False,
            "auto.offset.reset": "latest",
            "allow.auto.create.topics": False,
        }
    )
    consumer.subscribe([args.topic])
    deadline = time.monotonic() + 15
    while time.monotonic() < deadline and not consumer.assignment():
        message = consumer.poll(0.5)
        if message is not None and message.error():
            raise RuntimeError(str(message.error()))
    result["assigned_partitions"] = len(consumer.assignment())
    if not result["assigned_partitions"]:
        raise RuntimeError("Metadata succeeded, but the consumer received no partition assignment")
    result["connected"] = True
except Exception as exc:
    result["error"] = str(exc)
finally:
    if consumer:
        consumer.close()
body = json.dumps(result, indent=2) + "\n"
print(body, end="")
if args.output:
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(body)
raise SystemExit(0 if result["connected"] else 1)
````

## scripts/generate-group-tokens.sh

Existing token-generation helper; generated credentials are excluded.

SHA256: `30e64593ed4986e5ce4ec504a48e76fb78124501c0a078c80139b0eb217866fb`

````sh
#!/bin/bash

USAGE="
Usage: generate-group-tokens.sh <num-groups>

    num-groups: integer number of groups
"

if (( $# != 1 )); then
    cat <<< "$USAGE"
    exit 1
fi 


if ! [[ "$1" =~ ^[0-9]+$ ]]; then
    cat <<< "$USAGE"
    exit 1
fi

num_groups="$1"

for (( i=0; i<$num_groups; i++ )); do
    echo "=== group$i ==="
    cargo run -p notifications-service --bin generate-jwt-token -- --client-id "group${i}" > "../cec-infrastructure-2024/creds/groups/group${i}/token"
done
````

## scripts/prepare-monitoring-credentials.py

Prepare the narrow mounted credential files without printing their private values.

SHA256: `a873f29e9dbf11ee1f2e3699ced34c4e47c10c5823423340ada5e20e32d8775b`

````python
"""Extract the Kafka keystore password without printing or committing credentials."""

import argparse
import os
from pathlib import Path

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("directory", nargs="?", default="group15-credentials")
args = parser.parse_args()
directory = Path(args.directory)
properties = {}
for line in (directory / "client-ssl.properties").read_text().splitlines():
    if "=" in line and not line.lstrip().startswith("#"):
        key, value = line.split("=", 1)
        properties[key.strip()] = value.strip()
password = properties.get("ssl.keystore.password")
if not password:
    raise SystemExit("client-ssl.properties is missing ssl.keystore.password")
path = directory / "keystore-password"
fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
with os.fdopen(fd, "w") as output:
    output.write(password + "\n")
path.chmod(0o600)
print(f"Prepared password file. Credential owner UID={os.getuid()}, GID={os.getgid()}.")
print("The demo overlay defaults to UID/GID 1000; set TO_CREDENTIAL_UID/GID if different.")
````

## scripts/push.sh

Existing course image-push helper; not executed to prepare this pack.

SHA256: `31e42fc681f15fe5e8a266980066424a39867c0ca9508ed298f44e1333335549`

````sh
#!/bin/bash

packages=(
    notifications-service 
    experiment-producer 
    http-load-generator 
    notifier 
    test-to-api
)

for package in ${packages[@]}; do
    echo "=== Pushing $package ==="
    docker push "dclandau/demo-${package}"
done

````

## scripts/save\_env.sh

Existing environment helper; runtime secret files are excluded from the report.

SHA256: `49e794c3bac88bd30c4a29558651b41fb87cff2f5d279a46b3b6e1ca32db280f`

````sh
#!/bin/bash

copy_file_to_dst() {
    dir="$(dirname $2)"
    file="$(basename $2)"
    if ! [[ -d "$dir" ]]; then
        mkdir -p $dir
    fi 
    cp "$1" $2
}

USAGE='
Usage: save_env.sh <environment>

    environment: Save the current configuration files into an environment in the repository'"'"'s root `config` directory
'

if [[ -z "$1" ]]; then
    echo "$USAGE"
    exit 1
fi 

script_dir="$(cd -- $(dirname ${BASH_SOURCE[0]}) && pwd)"
repo_dir="$(dirname "$script_dir")"


template="${repo_dir}/config/template"
environment="${repo_dir}/config/$1"
files=$(find "$template" -type f )
for file in $files; do
    relative_file="${file#"$template/"}"
    src="${repo_dir}/${relative_file}"
    dst="${repo_dir}/config/$1/${relative_file}"
    copy_file_to_dst "$src" "$dst"
done
````

## scripts/setup\_env.sh

Existing development environment helper; inclusion does not claim new authorship.

SHA256: `adbcf7d612d3a0f1f24f465f4e7b1cc661ebecc53ca8e3913d5092aa18ee2084`

````sh
#!/bin/bash

copy_file_to_dst() {
    dir="$(dirname $2)"
    file="$(basename $2)"
    if ! [[ -d "$dir" ]]; then
        mkdir -p $dir
    fi 
    cp "$1" $2
}

USAGE='
Usage: setup_config.sh <environment>

    environment: One of the environments available on the repository'"'"'s root
        directory `<repository-root>/config` path
'

if [[ -z "$1" ]]; then
    echo "$USAGE"
    exit 1
fi 

script_dir="$(cd -- $(dirname ${BASH_SOURCE[0]}) && pwd)"
repo_dir="$(dirname "$script_dir")"

environment="${repo_dir}/config/$1"
if ! [[ -d "$environment" ]]; then
    echo "$USAGE"
    exit 1
fi

files=$(find "$environment" -type f )
for src in $files; do
    relative_file="${src#"$environment/"}"
    dst="${repo_dir}/${relative_file}"
    copy_file_to_dst "$src" "$dst"
done
````

## temperature\_observability/\_\_init\_\_.py

Package marker; no standalone service behavior.

SHA256: `b181706c32a94873855e583d77402faaf924307fd73d2d483d1dc4173a95f10f`

````python
"""XYZ Lab temperature observability services."""
````

## temperature\_observability/api.py

Development REST/OpenAPI contract; deployed Caddy routes implement the public path.

SHA256: `773b805ba7c73ba802af876329bfd3c9c5073237103e099bde670373ffc82792`

````python
from contextlib import asynccontextmanager
from math import isfinite
from urllib.parse import quote

import httpx
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import FiniteFloat

from .common import Metrics, configure_logging, http_client
from .config import Settings
from .models import ExperimentConfig, TemperaturePoint


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client() as client:
            app.state.client = client
            yield

    app = FastAPI(title="XYZ Lab Temperature Observability", version="1.0.0", lifespan=lifespan)
    metrics.install(app)

    async def forward(method, path, *, base=None, **kwargs):
        try:
            response = await app.state.client.request(
                method, (base or settings.data_url) + path, **kwargs
            )
        except httpx.HTTPError as exc:
            raise HTTPException(503, "monitoring service temporarily unavailable") from exc
        return Response(
            response.content, status_code=response.status_code, media_type="application/json"
        )

    @app.get("/health")
    async def health():
        return await forward("GET", "/health")

    @app.post(
        "/experiments",
        summary="Configure an experiment; the Kafka configuration event is also supported",
    )
    async def configure(config: ExperimentConfig):
        return await forward(
            "POST",
            "/configure",
            base=f"http://{settings.executor_host}:{settings.executor_port}",
            json=config.model_dump(),
        )

    @app.get("/experiments/{experiment_id}")
    async def experiment(experiment_id: str):
        return await forward("GET", "/experiments/" + quote(experiment_id, safe=""))

    @app.get("/temperature", response_model=list[TemperaturePoint])
    async def temperature(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
        start: FiniteFloat = Query(alias="start-time", ge=0),
        end: FiniteFloat = Query(alias="end-time", ge=0),
    ):
        if not isfinite(start) or not isfinite(end) or start > end:
            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
        return await forward(
            "GET",
            "/temperature",
            params={
                "experiment-id": experiment_id,
                "start-time": start,
                "end-time": end,
            },
        )

    @app.get("/temperature/out-of-range", response_model=list[TemperaturePoint])
    async def out_of_range(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await forward(
            "GET", "/temperature/out-of-range", params={"experiment-id": experiment_id}
        )

    @app.get("/temperature/out-of-range/intervals")
    async def intervals(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await forward(
            "GET", "/temperature/out-of-range/intervals", params={"experiment-id": experiment_id}
        )

    return app


app = create_app()
````

## temperature\_observability/autoscale.py

Opt-in bounded host policy with busy/idle hysteresis and dry-run; not continuously active in demo.

SHA256: `c67ef53c42fdcdd50d201b970435cf6575af34e4d9aa1b76b7cce47ef3a8a270`

````python
"""Run on the Docker host to adjust executor replicas to sustained demand."""

import argparse
import json
import subprocess
import time
import urllib.request
from dataclasses import dataclass


@dataclass
class ScalingPolicy:
    minimum: int = 1
    maximum: int = 4
    up_after: float = 5
    down_after: float = 60
    busy_since: float | None = None
    idle_since: float | None = None

    def decide(self, status: dict, now: float) -> int:
        current = max(self.minimum, status["executors"])
        busy = status["queued_events"] > 1024 or status["active_tasks"] >= 0.75 * max(
            1, status["capacity"]
        )
        idle = status["queued_events"] == 0 and status["active_tasks"] == 0
        self.busy_since = (
            (self.busy_since if self.busy_since is not None else now) if busy else None
        )
        self.idle_since = (
            (self.idle_since if self.idle_since is not None else now) if idle else None
        )
        if self.busy_since is not None and now - self.busy_since >= self.up_after:
            self.busy_since = now
            return min(self.maximum, current + 1)
        if self.idle_since is not None and now - self.idle_since >= self.down_after:
            self.idle_since = now
            return max(self.minimum, current - 1)
        return min(self.maximum, current)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--minimum", type=int, default=1)
    parser.add_argument("--maximum", type=int, default=4)
    parser.add_argument("--scheduler", default="http://localhost:8001")
    parser.add_argument("--compose-file", action="append", default=None)
    parser.add_argument("--env-file")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.minimum <= args.maximum:
        parser.error("require 1 <= minimum <= maximum")
    policy = ScalingPolicy(args.minimum, args.maximum)
    command = ["docker", "compose"]
    for filename in args.compose_file or ["compose.yml", "compose.local.yml"]:
        command.extend(["-f", filename])
    if args.env_file:
        command.extend(["--env-file", args.env_file])
    while True:
        try:
            with urllib.request.urlopen(args.scheduler + "/status", timeout=3) as response:
                status = json.load(response)
            desired = policy.decide(status, time.monotonic())
            if desired != status["executors"]:
                print(
                    json.dumps({"replicas": desired, "load": status, "dry_run": args.dry_run}),
                    flush=True,
                )
                if not args.dry_run:
                    subprocess.run(
                        [
                            *command,
                            "up",
                            "-d",
                            "--no-deps",
                            "--no-build",
                            "--scale",
                            f"executor={desired}",
                            "executor",
                        ],
                        check=True,
                        timeout=60,
                    )
                    time.sleep(5)
        except (OSError, ValueError, subprocess.SubprocessError) as exc:
            print(f"Autoscaler could not inspect/scale: {type(exc).__name__}", flush=True)
        time.sleep(2)


if __name__ == "__main__":
    main()
````

## temperature\_observability/codec.py

Avro object-container decoding and authoritative record_name header; returns explicit invalid envelopes.

SHA256: `5c3e5b950c696d571536a84c17736bcd0ede9469dc26741588a0cc41f7e6d57e`

````python
import io
import json

import fastavro

from .models import EventEnvelope, EventType


def decode_event(offset: int, headers: list | None, value: bytes | None) -> EventEnvelope:
    """The course producer writes Avro object containers, not Confluent wire framing.

    record_name is authoritative: its termination event embeds the started schema.
    Malformed records become durable rejected events rather than killing the consumer.
    """
    kind = "unknown"
    try:
        name = dict(headers or []).get("record_name")
        if name is None:
            raise ValueError("missing record_name header")
        kind = name.decode("utf-8") if isinstance(name, bytes) else name
        if kind not in EventType:
            raise ValueError("unknown record_name header")
        if not value or not value.startswith(b"Obj\x01"):
            raise ValueError("expected an Avro object container")
        records = list(fastavro.reader(io.BytesIO(value)))
        if not records or not all(isinstance(r, dict) for r in records):
            raise ValueError("Avro container must contain event records")
        # JSONB cannot store NaN/Infinity or Avro logical types such as datetime.
        # Reject these before they can become an indefinitely retried persistence error.
        json.dumps(records, allow_nan=False)
        return EventEnvelope(offset=offset, kind=kind, records=records)
    except Exception as exc:
        # Do not log message content, ciphertext, or credentials.
        return EventEnvelope(
            offset=offset, kind=kind, error=f"{type(exc).__name__}: invalid Avro event"
        )
````

## temperature\_observability/common.py

Shared metrics, asynchronous pooled transport, logging and supervised service tasks; transport tests guard compatibility.

SHA256: `b2fcbc01facbc73c63d11613a6fee026e30b62887bc742c4327e7f35a2a72140`

````python
import asyncio
import logging
import time

import aiohttp
import httpx
from fastapi import FastAPI, Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    GCCollector,
    Histogram,
    PlatformCollector,
    ProcessCollector,
    generate_latest,
)


def configure_logging():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    # Notification URLs contain tokens. Never emit HTTP client access logs.
    logging.getLogger("httpx").setLevel(logging.WARNING)
    logging.getLogger("httpcore").setLevel(logging.WARNING)


class ServiceClient:
    """Pooled aiohttp transport with HTTPX response/error types at service boundaries.

    The transport avoids HTTPX's per-connection asynchronous lock overhead on this
    small VM. Response objects also make service handlers easy to test in isolation.
    """

    def __init__(self, timeout: float = 10):
        self.session = aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=timeout, connect=2),
            connector=aiohttp.TCPConnector(limit=128, keepalive_timeout=30, ttl_dns_cache=2),
            trust_env=False,
            auto_decompress=False,  # HTTPX decodes response bodies using the original headers.
        )

    async def request(self, method, url, **kwargs):
        kwargs.setdefault("allow_redirects", False)
        if "content" in kwargs:
            kwargs["data"] = kwargs.pop("content")
        timeout = kwargs.pop("timeout", None)
        if timeout is not None:
            kwargs["timeout"] = aiohttp.ClientTimeout(total=timeout, connect=min(2, timeout))
        request = httpx.Request(method, url)
        try:
            async with self.session.request(method, url, **kwargs) as response:
                return httpx.Response(
                    response.status,
                    headers=response.headers,
                    content=await response.read(),
                    request=request,
                )
        except (aiohttp.ClientError, TimeoutError) as exc:
            raise httpx.TransportError("upstream request failed", request=request) from exc

    async def get(self, url, **kwargs):
        return await self.request("GET", url, **kwargs)

    async def post(self, url, **kwargs):
        return await self.request("POST", url, **kwargs)

    async def aclose(self):
        await self.session.close()

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        await self.aclose()


def http_client(timeout: float = 10):
    return ServiceClient(timeout)


async def cancel_tasks(*tasks: asyncio.Task):
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


async def supervise(run, name):
    """Restart background loops after an unexpected failure; cancellation still stops them."""
    while True:
        try:
            await run()
        except Exception:
            logging.getLogger(__name__).exception("Restarting %s after unexpected failure", name)
        await asyncio.sleep(1)


class Metrics:
    def __init__(self):
        self.registry = CollectorRegistry()
        ProcessCollector(registry=self.registry)
        GCCollector(registry=self.registry)
        PlatformCollector(registry=self.registry)
        self.http_duration = Histogram(
            "to_http_request_seconds",
            "HTTP request duration",
            ["route", "method", "status"],
            buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 10),
            registry=self.registry,
        )

    def counter(self, name, description, labels=()):
        return Counter(name, description, labels, registry=self.registry)

    def gauge(self, name, description, labels=()):
        return Gauge(name, description, labels, registry=self.registry)

    def histogram(self, name, description, labels=()):
        return Histogram(
            name,
            description,
            labels,
            buckets=(0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5, 8, 10, 15, 30, 60),
            registry=self.registry,
        )

    def install(self, app: FastAPI):
        @app.get("/metrics", include_in_schema=False)
        async def metrics():
            return Response(
                generate_latest(self.registry), headers={"Content-Type": CONTENT_TYPE_LATEST}
            )

        app.add_middleware(RequestMetricsMiddleware, histogram=self.http_duration)


class RequestMetricsMiddleware:
    """Observe ASGI messages directly, without spawning tasks or buffering responses."""

    def __init__(self, app, histogram):
        self.app, self.histogram = app, histogram

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] == "/metrics":
            return await self.app(scope, receive, send)
        start, status = time.monotonic(), 500

        async def measured_send(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, measured_send)
        finally:
            path = getattr(scope.get("route"), "path", "unmatched")
            self.histogram.labels(path, scope["method"], str(status)).observe(
                time.monotonic() - start
            )
````

## temperature\_observability/config.py

Validated service limits, Kafka/TLS options and one-VM plus provisioned-storage cost model.

SHA256: `ee725afc31f5f4dca088e020b4931ef18c9ebdf95d0d3f95d4449c525c5a1fef`

````python
from pathlib import Path

from pydantic import Field, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_prefix="TO_", extra="ignore")

    database_url: str = "postgresql://observability:observability@postgres:5432/observability"
    redis_url: str = "redis://redis:6379/0"
    data_url: str = "http://data:8000"
    executor_host: str = "executor"
    executor_port: int = 8000
    executor_capacity: int = Field(default=8, ge=1)
    executor_replicas: int = Field(default=1, ge=1)
    batch_size: int = Field(default=256, ge=1, le=1024)
    queue_capacity: int = Field(default=8192, ge=1024)
    max_pending_measurements: int = Field(default=256, ge=1)
    kafka_brokers: str = "kafka:9092"
    kafka_topic: str = "experiment"
    kafka_group: str = "group15-temperature-v1"
    kafka_security_protocol: str = "PLAINTEXT"
    kafka_ca_file: str = ""
    kafka_keystore_file: str = ""
    kafka_keystore_password_file: str = ""
    kafka_endpoint_identification: str = "https"
    notification_url: str = "http://notification-sink:8000/api/notify"
    notification_token_file: str = ""
    notification_interval: float = Field(default=0.1, gt=0, le=5)
    notification_concurrency: int = Field(default=32, ge=1, le=256)
    notification_timeout: float = Field(default=2, gt=0)
    notification_lease: float = Field(default=15, gt=0)
    cache_ttl: int = Field(default=30, ge=1)
    history_cache_points: int = Field(default=16384, ge=0)
    history_snapshot_limit: int = Field(default=512, ge=1, le=4096)
    vm_hourly_cost_usd: float = Field(default=0, ge=0)
    storage_gb_monthly_cost_usd: float = Field(default=0, ge=0)
    storage_provisioned_gb: float = Field(default=0, ge=0)

    @property
    def estimated_hourly_cost_usd(self):
        # EBS bills provisioned capacity, not PostgreSQL's currently used bytes.
        # Replicas on this same VM do not each add another VM charge.
        return self.vm_hourly_cost_usd + (
            self.storage_provisioned_gb * self.storage_gb_monthly_cost_usd / 730
        )

    @property
    def cost_configured(self):
        return all(
            value > 0
            for value in (
                self.vm_hourly_cost_usd,
                self.storage_gb_monthly_cost_usd,
                self.storage_provisioned_gb,
            )
        )

    @model_validator(mode="after")
    def check_lease(self):
        if self.notification_lease < self.notification_timeout * 2 + 2:
            raise ValueError("notification lease must exceed twice the HTTP timeout plus 2 seconds")
        return self

    def kafka_config(self) -> dict:
        config = {
            "bootstrap.servers": self.kafka_brokers,
            "security.protocol": self.kafka_security_protocol,
        }
        if self.kafka_security_protocol == "SSL":
            config.update(
                {
                    "ssl.ca.location": self.kafka_ca_file,
                    "ssl.keystore.location": self.kafka_keystore_file,
                    "ssl.endpoint.identification.algorithm": self.kafka_endpoint_identification,
                }
            )
            if self.kafka_keystore_password_file:
                config["ssl.keystore.password"] = (
                    Path(self.kafka_keystore_password_file).read_text().strip()
                )
        return config
````

## temperature\_observability/data.py

Authoritative atomic commit/load, version checks, query caches, writer fencing, outbox leases and diagnostic metrics.

SHA256: `dbd5b3b0216aeb8bd11cbe40440bfcfed7458fa4ae8f78ecb2520fb9fee2a576`

````python
import asyncio
import hashlib
import json
import logging
import time
from collections import Counter, OrderedDict
from contextlib import asynccontextmanager
from itertools import groupby
from math import isfinite
from pathlib import Path
from uuid import uuid4

import asyncpg
from fastapi import FastAPI, HTTPException, Query, Response
from pydantic import FiniteFloat
from redis.asyncio import Redis
from redis.asyncio.retry import Retry
from redis.backoff import NoBackoff
from redis.exceptions import RedisError

from .common import Metrics, cancel_tasks, configure_logging
from .config import Settings
from .history import HistoryCache, HistorySnapshot
from .models import ClaimRequest, Commit, NotificationResult, Snapshot, StoredState, TaskLookup

log = logging.getLogger(__name__)


async def init_connection(connection):
    for typename in ("json", "jsonb"):
        await connection.set_type_codec(
            typename, encoder=json.dumps, decoder=json.loads, schema="pg_catalog"
        )


async def reset_connection(connection):
    # asyncpg still rolls back unfinished transactions before this callback.
    # This service never sets session variables, LISTENs, opens SQL cursors, or
    # takes session advisory locks, so its default session-reset SQL is redundant.
    pass


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()
    cache_reads = metrics.counter("to_cache_reads_total", "History cache results", ["result"])
    outbox_count = metrics.gauge("to_notification_jobs", "Durable notification jobs", ["status"])
    outbox_age = metrics.gauge(
        "to_notification_oldest_pending_seconds", "Age of oldest pending alert"
    )
    rejected_count = metrics.gauge("to_rejected_events", "Durably quarantined Kafka messages")
    storage = metrics.gauge("to_database_bytes", "PostgreSQL database size")
    cost = metrics.gauge(
        "to_estimated_hourly_cost_usd",
        "Estimated VM and provisioned storage cost, excluding credits/network/tax",
    )
    cost_configured = metrics.gauge(
        "to_cost_configured", "Whether VM rate, storage rate and provisioned size are supplied"
    )
    pool_wait = metrics.histogram(
        "to_database_pool_wait_seconds",
        "Time waiting to acquire a database connection",
        ["operation"],
    )
    connection_time = metrics.histogram(
        "to_database_connection_seconds",
        "Time holding a connection, including transaction work",
        ["operation"],
    )
    pool_connections = metrics.gauge(
        "to_database_pool_connections", "Database pool size", ["state"]
    )

    @asynccontextmanager
    async def connection(operation):
        started = time.monotonic()
        async with app.state.pool.acquire() as conn:
            acquired = time.monotonic()
            pool_wait.labels(operation).observe(acquired - started)
            try:
                yield conn
            finally:
                connection_time.labels(operation).observe(time.monotonic() - acquired)

    async def update_metrics():
        next_full_count = 0
        while True:
            try:
                pool = app.state.pool
                pool_connections.labels("open").set(pool.get_size())
                pool_connections.labels("idle").set(pool.get_idle_size())
                pool_connections.labels("limit").set(pool.get_max_size())
                async with connection("metrics") as conn:
                    full_count = time.monotonic() >= next_full_count
                    # Alerting needs current pending/failed counts. Historical
                    # sent/rejected totals need not rescan years of data every 5s.
                    rows = await conn.fetch(
                        "SELECT status, count(*) AS n FROM notification_outbox "
                        + ("" if full_count else "WHERE status IN ('pending','failed') ")
                        + "GROUP BY status"
                    )
                    counts = {r["status"]: r["n"] for r in rows}
                    for status in (
                        ("pending", "sent", "failed") if full_count else ("pending", "failed")
                    ):
                        outbox_count.labels(status).set(counts.get(status, 0))
                    age = await conn.fetchval("""
                        SELECT greatest(0, extract(epoch FROM now()) - min(measured_at))
                        FROM notification_outbox WHERE status = 'pending'
                    """)
                    outbox_age.set(age or 0)
                    if full_count:
                        rejected_count.set(
                            await conn.fetchval("SELECT count(*) FROM rejected_events")
                        )
                        next_full_count = time.monotonic() + 60
                    size = await conn.fetchval("SELECT pg_database_size(current_database())")
                    storage.set(size)
                    cost.set(
                        settings.estimated_hourly_cost_usd
                        if settings.cost_configured
                        else float("nan")
                    )
                    cost_configured.set(int(settings.cost_configured))
            except Exception:
                log.warning("Data metrics collection failed", exc_info=True)
            await asyncio.sleep(5)

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        app.state.history_metadata = OrderedDict()
        app.state.metadata_epoch = 0
        app.state.metadata_writers = Counter()
        app.state.metadata_generations = {}
        app.state.histories = HistoryCache(settings.history_cache_points)
        app.state.snapshot_locks = {}
        app.state.pool = await asyncpg.create_pool(
            settings.database_url,
            min_size=2,
            max_size=24,
            init=init_connection,
            reset=reset_connection,
            command_timeout=20,
        )
        async with app.state.pool.acquire() as conn, conn.transaction():
            await conn.execute("SELECT pg_advisory_xact_lock(151503)")
            await conn.execute(Path(__file__).with_name("schema.sql").read_text())
        app.state.cache = Redis.from_url(
            settings.redis_url,
            socket_connect_timeout=0.1,
            socket_timeout=0.1,
            decode_responses=True,
            retry=Retry(NoBackoff(), retries=0),
        )
        app.state.cache_unavailable_until = 0
        updater = asyncio.create_task(update_metrics())
        yield
        await cancel_tasks(updater)
        await app.state.cache.aclose()
        await app.state.pool.close()

    app = FastAPI(title="Temperature data service (internal)", lifespan=lifespan)
    metrics.install(app)

    @asynccontextmanager
    async def invalidate_metadata(experiments):
        # Data is the single writer. Fence cache fills for the entire transaction,
        # including rollback/cancellation, and discard metadata on both boundaries.
        app.state.metadata_epoch += 1
        generation = app.state.metadata_epoch
        warmable = set()
        for experiment in experiments:
            app.state.history_metadata.pop(experiment, None)
            app.state.metadata_writers[experiment] += 1
            app.state.metadata_generations[experiment] = generation
        try:
            yield warmable
        finally:
            app.state.metadata_epoch += 1
            for experiment in experiments:
                app.state.history_metadata.pop(experiment, None)
                app.state.metadata_writers[experiment] -= 1
                if app.state.metadata_writers[experiment] == 0:
                    if app.state.metadata_generations[experiment] == generation:
                        warmable.add(experiment)
                    del app.state.metadata_writers[experiment]
                    del app.state.metadata_generations[experiment]

    def remember_metadata(experiment, row):
        app.state.history_metadata[experiment] = row
        app.state.history_metadata.move_to_end(experiment)
        if len(app.state.history_metadata) > 4096:
            app.state.history_metadata.popitem(last=False)

    @asynccontextmanager
    async def snapshot_lock(experiment):
        # Count users before awaiting; remove locks only once all waiters leave.
        entry = app.state.snapshot_locks.setdefault(experiment, [asyncio.Lock(), 0])
        entry[1] += 1
        try:
            async with entry[0]:
                yield
        finally:
            entry[1] -= 1
            if entry[1] == 0:
                del app.state.snapshot_locks[experiment]

    @app.get("/health")
    async def health():
        try:
            async with connection("health") as conn:
                await conn.fetchval("SELECT 1")
        except Exception as exc:
            raise HTTPException(503, "PostgreSQL unavailable") from exc
        # Cache failure degrades speed, not availability or correctness.
        return {"status": "ok"}

    @app.post("/internal/tasks/load", response_model=Snapshot)
    async def load(task: TaskLookup):
        async with (
            connection("load") as conn,
            conn.transaction(isolation="repeatable_read", readonly=True),
        ):
            checkpoint = -1
            if task.stream is not None:
                checkpoint = await conn.fetchval(
                    "SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2",
                    task.stream,
                    task.partition,
                )
                checkpoint = -1 if checkpoint is None else checkpoint
            rows = await conn.fetch(
                "SELECT id, version, state FROM experiments WHERE id=ANY($1::text[])",
                task.experiments,
            )
            completed = []
            if task.measurements:
                experiments, measurement_ids = zip(*task.measurements, strict=True)
                seen = await conn.fetch(
                    """
                    SELECT m.experiment_id, m.measurement_id FROM measurements m
                    JOIN unnest($1::text[], $2::text[]) AS ids(experiment, measurement)
                    ON m.experiment_id=ids.experiment AND m.measurement_id=ids.measurement
                """,
                    list(experiments),
                    list(measurement_ids),
                )
                completed = [(r["experiment_id"], r["measurement_id"]) for r in seen]
            return Snapshot(
                checkpoint=checkpoint,
                states={r["id"]: StoredState(version=r["version"], state=r["state"]) for r in rows},
                completed=completed,
            )

    @app.post("/internal/tasks/commit")
    async def commit(change: Commit):
        committed_metadata = {}
        async with (
            invalidate_metadata(change.states) as warmable,
            connection("commit") as conn,
            conn.transaction(),
        ):
            if change.stream is not None:
                await conn.execute(
                    """
                    INSERT INTO checkpoints VALUES ($1,$2,-1) ON CONFLICT DO NOTHING
                """,
                    change.stream,
                    change.partition,
                )
                checkpoint = await conn.fetchval(
                    """
                    SELECT last_offset FROM checkpoints WHERE stream=$1 AND partition=$2 FOR UPDATE
                """,
                    change.stream,
                    change.partition,
                )
                if checkpoint != change.expected_checkpoint:
                    raise HTTPException(409, "partition checkpoint changed; reload task")
                if change.checkpoint < checkpoint:
                    raise HTTPException(400, "checkpoint cannot move backwards")
            # Pipeline adjacent writes of the same kind instead of paying a
            # database round trip per experiment. Keep the global ID ordering
            # for row locks and require every optimistic version check to pass.
            for creating, entries in groupby(
                sorted(change.states.items()), key=lambda item: item[1].version == -1
            ):
                entries = list(entries)
                if creating:
                    rows = await conn.fetchmany(
                        """
                        INSERT INTO experiments(id, version, state) VALUES ($1,0,$2)
                        ON CONFLICT DO NOTHING RETURNING id,version
                    """,
                        [(experiment, saved.state.model_dump()) for experiment, saved in entries],
                    )
                else:
                    rows = await conn.fetchmany(
                        """
                        UPDATE experiments SET version=version+1, state=$3, updated_at=now()
                        WHERE id=$1 AND version=$2 RETURNING id,version
                    """,
                        [
                            (experiment, saved.version, saved.state.model_dump())
                            for experiment, saved in entries
                        ],
                    )
                if len(rows) != len(entries):
                    raise HTTPException(409, "experiment version changed; reload task")
                for row in rows:
                    state = change.states[row["id"]].state
                    committed_metadata[row["id"]] = {
                        "version": row["version"],
                        "ended": state.terminated_at,
                        "latest": state.last_timestamp,
                    }
            if change.measurements:
                await conn.executemany(
                    """
                    INSERT INTO measurements VALUES ($1,$2,$3,$4,$5,$6)
                    ON CONFLICT (experiment_id,measurement_id) DO NOTHING
                """,
                    [
                        (
                            m.experiment_id,
                            m.measurement_id,
                            m.timestamp,
                            m.temperature,
                            m.phase,
                            m.out_of_range,
                        )
                        for m in change.measurements
                    ],
                )
            if change.notifications:
                await conn.executemany(
                    """
                    INSERT INTO notification_outbox
                    (experiment_id,measurement_id,notification_type,payload,measured_at)
                    VALUES ($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING
                """,
                    [
                        (
                            n.payload.experiment_id,
                            n.payload.measurement_id,
                            n.payload.notification_type,
                            n.payload.model_dump(),
                            n.measured_at,
                        )
                        for n in change.notifications
                    ],
                )
            if change.rejected:
                await conn.executemany(
                    """
                    INSERT INTO rejected_events(stream,partition,offset_id,kind,reason,records)
                    VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING
                """,
                    [
                        (change.stream, change.partition, r.offset, r.kind, r.reason, r.records)
                        for r in change.rejected
                    ],
                )
            if change.stream is not None:
                await conn.execute(
                    """
                    UPDATE checkpoints SET last_offset=$3 WHERE stream=$1 AND partition=$2
                """,
                    change.stream,
                    change.partition,
                    change.checkpoint,
                )
        # The successful transaction already knows these values. Avoid a SELECT
        # on the first history read after every update. Only warm when this was
        # the last uncontended writer: a delayed pool release must not overwrite
        # metadata belonging to a newer commit. No await between fence and fill.
        for experiment in warmable:
            remember_metadata(experiment, committed_metadata[experiment])
        return {"status": "committed", "checkpoint": change.checkpoint}

    @app.get("/experiments/{experiment_id}")
    async def get_experiment(experiment_id: str):
        async with connection("experiment") as conn:
            row = await conn.fetchrow(
                "SELECT state,version FROM experiments WHERE id=$1", experiment_id
            )
        if row is None:
            raise HTTPException(404, "unknown experiment")
        state = row["state"]
        return {
            **state["config"],
            "status": state["status"],
            "version": row["version"],
            **{
                k: state[k]
                for k in ("started_at", "terminated_at", "last_timestamp", "last_temperature")
            },
            "pending_measurements": len(state["pending"]),
        }

    async def query_history(experiment_id, start=None, end=None, outside=False, intervals=False):
        if start is not None and (not isfinite(start) or not isfinite(end) or start > end):
            raise HTTPException(422, "times must be finite and start-time must be <= end-time")
        metadata = "version,state->>'terminated_at' AS ended,state->>'last_timestamp' AS latest"

        def key_for(row):
            # Queries ending after the latest aggregate have identical results at
            # this version. Reuse the cache when a client repeatedly asks 'until now'.
            cache_end = min(end, float(row["latest"] or 0)) if end is not None else None
            return (
                "history:"
                + hashlib.sha256(
                    json.dumps(
                        [experiment_id, row["version"], start, cache_end, outside, intervals]
                    ).encode()
                ).hexdigest()
            )

        row = app.state.history_metadata.get(experiment_id)
        if row is None:
            epoch = app.state.metadata_epoch
            async with connection("history_metadata") as conn:
                row = await conn.fetchrow(
                    f"SELECT {metadata} FROM experiments WHERE id=$1", experiment_id
                )
            if (
                row is not None
                and epoch == app.state.metadata_epoch
                and not app.state.metadata_writers[experiment_id]
            ):
                remember_metadata(experiment_id, row)
        else:
            app.state.history_metadata.move_to_end(experiment_id)
        if row is None:
            raise HTTPException(404, "unknown experiment")
        snapshots = app.state.histories
        if settings.history_cache_points and not snapshots.is_large(experiment_id):
            async with snapshot_lock(experiment_id):
                snapshot = snapshots.get(experiment_id, row["version"])
                if snapshot is not None:
                    cache_reads.labels("snapshot_hit").inc()
                elif not snapshots.is_large(experiment_id):
                    # Fetch at most limit+1 points in the SAME MVCC snapshot as
                    # the version. Never load years of data for a narrow query.
                    async with connection("history_snapshot") as conn:
                        saved = await conn.fetchrow(
                            f"""
                            SELECT {metadata}, (
                                SELECT coalesce(json_agg(point),'[]'::json)::text FROM (
                                    SELECT timestamp,temperature,out_of_range FROM measurements
                                    WHERE experiment_id=$1 AND phase='running'
                                    ORDER BY timestamp,measurement_id LIMIT $2
                                ) point
                            ) AS body FROM experiments WHERE id=$1
                        """,
                            experiment_id,
                            settings.history_snapshot_limit + 1,
                        )
                    points = json.loads(saved["body"])
                    if len(points) <= min(
                        settings.history_snapshot_limit, settings.history_cache_points
                    ):
                        snapshot = HistorySnapshot(
                            saved["version"],
                            float(saved["ended"]) if saved["ended"] is not None else None,
                            points,
                        )
                        snapshots.put(experiment_id, snapshot)
                        cache_reads.labels("snapshot_fill").inc()
                    else:
                        snapshots.put(experiment_id, None)
                        cache_reads.labels("snapshot_too_large").inc()
                if snapshot is not None:
                    return Response(
                        snapshot.render(start, end, outside, intervals),
                        media_type="application/json",
                    )
        if time.monotonic() >= app.state.cache_unavailable_until:
            try:
                cached = await app.state.cache.get(key_for(row))
                if cached is not None:
                    cache_reads.labels("hit").inc()
                    return Response(cached, media_type="application/json")
            except RedisError:
                app.state.cache_unavailable_until = time.monotonic() + 2
                cache_reads.labels("unavailable").inc()
        cache_reads.labels("miss").inc()
        clauses, params = ["experiment_id=$1", "phase='running'"], [experiment_id]
        if start is not None:
            clauses.append("timestamp >= $2 AND timestamp <= $3")
            params += [start, end]
        if outside:
            clauses.append("out_of_range")
        columns = "timestamp,temperature" + (",out_of_range" if intervals else "")
        # One SQL statement gives metadata and points the same MVCC snapshot.
        # PostgreSQL emits the wire JSON; cache I/O never holds a database connection.
        async with connection("history") as conn:
            row = await conn.fetchrow(
                f"""
                SELECT {metadata}, (
                    SELECT coalesce(json_agg(point),'[]'::json)::text FROM (
                        SELECT {columns} FROM measurements WHERE {" AND ".join(clauses)}
                        ORDER BY timestamp,measurement_id
                    ) point
                ) AS body FROM experiments WHERE id=$1
            """,
                *params,
            )
        if row is None:
            raise HTTPException(404, "unknown experiment")
        body = row["body"]
        if intervals:
            result, active = [], None
            for point in json.loads(body):
                if point["out_of_range"] and active is None:
                    active = {"start_time": point["timestamp"], "end_time": None}
                    result.append(active)
                elif not point["out_of_range"] and active is not None:
                    active["end_time"] = point["timestamp"]
                    active = None
            if active is not None and row["ended"] is not None:
                active["end_time"] = float(row["ended"])
            body = json.dumps(result, separators=(",", ":"))
        if time.monotonic() >= app.state.cache_unavailable_until:
            try:
                await app.state.cache.set(key_for(row), body, ex=settings.cache_ttl)
            except RedisError:
                app.state.cache_unavailable_until = time.monotonic() + 2
                cache_reads.labels("unavailable").inc()
        return Response(body, media_type="application/json")

    @app.get("/temperature")
    async def temperature(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
        start: FiniteFloat = Query(alias="start-time", ge=0),
        end: FiniteFloat = Query(alias="end-time", ge=0),
    ):
        return await query_history(experiment_id, start, end)

    @app.get("/temperature/out-of-range")
    async def out_of_range(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await query_history(experiment_id, outside=True)

    @app.get("/temperature/out-of-range/intervals")
    async def intervals(
        experiment_id: str = Query(alias="experiment-id", min_length=1, max_length=200),
    ):
        return await query_history(experiment_id, intervals=True)

    @app.post("/internal/notifications/claim")
    async def claim(request: ClaimRequest):
        token = str(uuid4())
        async with connection("claim") as conn:
            rows = await conn.fetch(
                """
                WITH selected AS (
                    SELECT id FROM notification_outbox
                    WHERE status='pending' AND next_attempt_at<=now()
                      AND (lease_until IS NULL OR lease_until<=now())
                    ORDER BY next_attempt_at,id FOR UPDATE SKIP LOCKED LIMIT $1
                )
                UPDATE notification_outbox n SET attempts=n.attempts+1, lease_token=$2,
                    lease_until=now()+$3*interval '1 second'
                FROM selected WHERE n.id=selected.id
                RETURNING n.id,n.payload,n.measured_at,n.attempts,n.lease_token
            """,
                request.limit,
                token,
                request.lease_seconds,
            )
        return [dict(row) for row in rows]

    @app.post("/internal/notifications/{job_id}/result")
    async def notification_result(job_id: int, result: NotificationResult):
        async with connection("acknowledge") as conn:
            updated = await conn.fetchval(
                """
                UPDATE notification_outbox SET
                    status=$3, lease_until=NULL, lease_token=NULL, last_status=$4,
                    latency=$5, delivered_at=CASE WHEN $3='sent' THEN now() ELSE NULL END,
                    next_attempt_at=now()+$6*interval '1 second'
                WHERE id=$1 AND lease_token=$2 RETURNING id
            """,
                job_id,
                result.lease_token,
                "pending" if result.outcome == "retry" else result.outcome,
                result.status_code,
                result.latency,
                result.retry_after,
            )
        if updated is None:
            raise HTTPException(409, "notification lease changed")
        return {"status": "ok"}

    return app


app = create_app()
````

## temperature\_observability/discovery.py

Discover executor addresses/load and reserve least-busy capacity without relying solely on stale observations.

SHA256: `359a233786c58005e77c25d9467feff638664b7d965a4d863f9e995ca622d2af`

````python
import asyncio
import socket
from dataclasses import dataclass

import httpx


@dataclass
class Worker:
    url: str
    active: int
    capacity: int
    reserved: int = 0
    last_selected: int = 0

    @property
    def load(self):
        return max(self.active, self.reserved)


class ExecutorPool:
    """Discover Compose replicas through DNS and reserve the least busy worker."""

    def __init__(self, host: str, port: int, client: httpx.AsyncClient):
        self.host, self.port, self.client = host, port, client
        self.workers: dict[str, Worker] = {}
        self.sequence = 0

    async def refresh(self):
        try:
            addresses = await asyncio.to_thread(
                socket.getaddrinfo, self.host, self.port, socket.AF_INET, socket.SOCK_STREAM
            )
        except OSError:
            self.workers = {}
            return
        urls = sorted({f"http://{address[4][0]}:{self.port}" for address in addresses})

        async def inspect(url):
            try:
                response = await self.client.get(url + "/load", timeout=1)
                response.raise_for_status()
                load = response.json()
                worker = self.workers.get(url, Worker(url, 0, 1))
                worker.active, worker.capacity = load["active"], load["capacity"]
                return worker
            except (httpx.HTTPError, ValueError, KeyError):
                return None

        found = await asyncio.gather(*(inspect(url) for url in urls))
        self.workers = {worker.url: worker for worker in found if worker is not None}

    def reserve(self) -> Worker | None:
        candidates = [w for w in self.workers.values() if w.load < w.capacity]
        if not candidates:
            return None
        worker = min(candidates, key=lambda w: (w.load, w.last_selected, w.url))
        self.sequence += 1
        worker.last_selected = self.sequence
        worker.reserved += 1
        return worker

    @staticmethod
    def release(worker):
        worker.reserved -= 1
````

## temperature\_observability/engine.py

Pure domain state machine: complete groups, ordered drain, phases, boundaries, deduplication and transition jobs.

SHA256: `137982ec58422ad6ec34cd8a0849dc77fdb329374e6db1512b1454018f13d082`

````python
"""Pure experiment state machine. No database, HTTP or Kafka dependencies."""

import math

from .models import (
    EventType,
    ExperimentConfig,
    ExperimentState,
    LifecycleEvent,
    Measurement,
    Notification,
    NotificationJob,
    PartialMeasurement,
    SensorEvent,
)


class InvalidEvent(ValueError):
    pass


class TemperatureEngine:
    @staticmethod
    def average(values: dict[str, float]) -> float:
        return math.fsum(values.values()) / len(values)

    @staticmethod
    def outside(state: ExperimentState, temperature: float) -> bool:
        limits = state.config.temperature_range
        return temperature < limits.lower_threshold or temperature > limits.upper_threshold


class ExperimentStateManager:
    def __init__(self, max_pending: int = 256):
        self.max_pending = max_pending

    def apply(
        self,
        state: ExperimentState | None,
        kind: str,
        payload: dict,
        completed: set[tuple[str, str]],
    ) -> tuple[ExperimentState, list[Measurement], list[NotificationJob]]:
        # Work on a copy so a rejected event cannot leave partially mutated state.
        state = (
            state.model_copy(update={"pending": dict(state.pending)}) if state is not None else None
        )
        if kind == EventType.CONFIGURED:
            config = ExperimentConfig.model_validate(payload)
            if state is not None:
                if state.config != config:
                    raise InvalidEvent("experiment already exists with a different configuration")
                return state, [], []
            return ExperimentState(config=config), [], []

        if state is None:
            raise InvalidEvent("experiment must be configured before receiving other events")
        if payload.get("experiment") != state.config.experiment:
            raise InvalidEvent("experiment ID does not match state")
        if kind != EventType.MEASURED:
            event = LifecycleEvent.model_validate(payload)
            transitions = {
                EventType.STABILIZATION: ("configured", "stabilizing"),
                EventType.STARTED: ("stabilizing", "running"),
                EventType.TERMINATED: ("running", "terminated"),
            }
            if kind not in transitions:
                raise InvalidEvent(f"unknown event type: {kind}")
            previous, following = transitions[kind]
            if state.status == following and state.phase_started_at == event.timestamp:
                return state, [], []
            # A researcher can start directly, or cancel before carry-out.
            allowed = {previous}
            if kind == EventType.STARTED:
                allowed.add("configured")
            if kind == EventType.TERMINATED:
                allowed.update({"configured", "stabilizing"})
            if state.status not in allowed:
                raise InvalidEvent(f"cannot apply {kind} while {state.status}")
            if state.pending:
                raise InvalidEvent("phase transition has incomplete sensor measurements")
            boundary = max(state.phase_started_at or 0, state.last_timestamp or 0)
            if event.timestamp < boundary:
                raise InvalidEvent("phase timestamp precedes an already processed event")
            state.status = following
            state.phase_started_at = event.timestamp
            state.previous_out_of_range = None
            if kind == EventType.STARTED:
                state.started_at = event.timestamp
            if kind == EventType.TERMINATED:
                state.terminated_at = event.timestamp
            return state, [], []

        # Accept the README's illustrative hyphenated spelling, too.
        payload = dict(payload)
        if "measurement-id" in payload and "measurement_id" not in payload:
            payload["measurement_id"] = payload.pop("measurement-id")
        event = SensorEvent.model_validate(payload)
        identity = (event.experiment, event.measurement_id)
        if identity in completed:
            return state, [], []
        if state.status not in {"stabilizing", "running"}:
            raise InvalidEvent(f"measurements are not allowed while {state.status}")
        if event.sensor not in state.config.sensors:
            raise InvalidEvent("sensor is not allocated to this experiment")
        if event.timestamp < max(state.phase_started_at or 0, state.last_timestamp or 0):
            raise InvalidEvent("measurement timestamp is older than processed state")
        partial = state.pending.get(event.measurement_id)
        if partial is not None:
            partial = partial.model_copy(update={"values": dict(partial.values)})
            state.pending[event.measurement_id] = partial
        if partial is None:
            if len(state.pending) >= self.max_pending:
                raise InvalidEvent("too many incomplete measurements; inspect missing sensors")
            partial = PartialMeasurement(
                timestamp=event.timestamp, measurement_hash=event.measurement_hash
            )
            state.pending[event.measurement_id] = partial
        if (
            partial.timestamp != event.timestamp
            or partial.measurement_hash != event.measurement_hash
        ):
            raise InvalidEvent("sensors disagree on measurement timestamp or measurement_hash")
        if event.sensor in partial.values and partial.values[event.sensor] != event.temperature:
            raise InvalidEvent("conflicting duplicate sensor reading")
        partial.values[event.sensor] = event.temperature

        measurements, notifications = [], []
        # Drain in time order. A later complete group waits for an earlier partial group.
        while state.pending:
            measurement_id = min(state.pending, key=lambda mid: (state.pending[mid].timestamp, mid))
            partial = state.pending[measurement_id]
            if len(partial.values) != len(state.config.sensors):
                break
            temperature = TemperatureEngine.average(partial.values)
            outside = TemperatureEngine.outside(state, temperature)
            notification_type = None
            if (
                state.status == "stabilizing"
                and not outside
                and state.previous_out_of_range is not False
            ):
                notification_type = "Stabilized"
            if state.status == "running" and outside and state.previous_out_of_range is not True:
                notification_type = "OutOfRange"
            measurements.append(
                Measurement(
                    experiment_id=event.experiment,
                    measurement_id=measurement_id,
                    timestamp=partial.timestamp,
                    temperature=temperature,
                    phase=state.status,
                    out_of_range=outside,
                )
            )
            if notification_type:
                notifications.append(
                    NotificationJob(
                        measured_at=partial.timestamp,
                        payload=Notification(
                            notification_type=notification_type,
                            researcher=state.config.researcher,
                            experiment_id=event.experiment,
                            measurement_id=measurement_id,
                            cipher_data=partial.measurement_hash,
                        ),
                    )
                )
            state.previous_out_of_range = outside
            state.last_timestamp = partial.timestamp
            state.last_temperature = temperature
            del state.pending[measurement_id]
        return state, measurements, notifications
````

## temperature\_observability/executor.py

Load snapshots, compute commit proposals, retry optimistic conflicts, enforce capacity; no authoritative worker-local state.

SHA256: `b1a568fdebb26408d0a01c9d317f9397f94b867c93783cd76f9f9977863f7e1c`

````python
import logging
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from pydantic import ValidationError

from .common import Metrics, configure_logging, http_client
from .config import Settings
from .engine import ExperimentStateManager, InvalidEvent
from .models import (
    Commit,
    EventEnvelope,
    EventType,
    ExperimentConfig,
    RejectedEvent,
    Snapshot,
    StoredState,
    TaskBatch,
    TaskLookup,
)

log = logging.getLogger(__name__)


def prepare_commit(task: TaskBatch, snapshot: Snapshot, max_pending: int, strict=False) -> Commit:
    manager = ExperimentStateManager(max_pending)
    states = {k: v.model_copy(deep=True) for k, v in snapshot.states.items()}
    changed, measurements, notifications, rejected = {}, [], [], []
    completed = set(snapshot.completed)
    for envelope in task.events:
        if task.stream is not None and envelope.offset <= snapshot.checkpoint:
            continue
        if envelope.error:
            rejected.append(
                RejectedEvent(
                    offset=envelope.offset, kind=envelope.kind, reason=envelope.error, records=[]
                )
            )
            continue
        for record in envelope.records:
            try:
                experiment = record.get("experiment")
                if not isinstance(experiment, str) or not experiment:
                    raise InvalidEvent("missing experiment ID")
                saved = states.get(experiment)
                state, new_measurements, new_notifications = manager.apply(
                    saved.state if saved else None,
                    envelope.kind,
                    record,
                    completed,
                )
                changed[experiment] = states[experiment] = StoredState(
                    version=saved.version if saved else -1,
                    state=state,
                )
                measurements.extend(new_measurements)
                notifications.extend(new_notifications)
                completed.update((m.experiment_id, m.measurement_id) for m in new_measurements)
            except (InvalidEvent, ValidationError) as exc:
                if strict:
                    raise HTTPException(
                        409 if isinstance(exc, InvalidEvent) else 422, str(exc)
                    ) from exc
                reason = str(exc) if isinstance(exc, InvalidEvent) else "invalid event fields"
                rejected.append(
                    RejectedEvent(
                        offset=envelope.offset,
                        kind=envelope.kind,
                        reason=reason,
                        records=[record],
                    )
                )
    return Commit(
        stream=task.stream,
        partition=task.partition,
        expected_checkpoint=snapshot.checkpoint,
        checkpoint=max(snapshot.checkpoint, task.events[-1].offset),
        states=changed,
        measurements=measurements,
        notifications=notifications,
        rejected=rejected,
    )


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()
    active = metrics.gauge("to_executor_active_tasks", "Currently executing task batches")
    capacity = metrics.gauge("to_executor_capacity", "Maximum concurrent batches")
    capacity.set(settings.executor_capacity)
    batches = metrics.counter("to_executor_batches_total", "Committed task batches")
    conflicts = metrics.counter("to_executor_conflicts_total", "Optimistic concurrency retries")
    rejected = metrics.counter("to_executor_rejected_events_total", "Quarantined invalid events")

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        app.state.client = http_client()
        app.state.active = 0
        yield
        await app.state.client.aclose()

    app = FastAPI(title="Stateless task executor (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    @app.get("/load")
    async def load():
        return {"active": app.state.active, "capacity": settings.executor_capacity}

    async def execute(task: TaskBatch, strict=False):
        if app.state.active >= settings.executor_capacity:
            raise HTTPException(429, "executor is at capacity")
        app.state.active += 1
        active.inc()
        try:
            lookup = TaskLookup.from_task(task).model_dump()
            for _ in range(8):
                response = await app.state.client.post(
                    settings.data_url + "/internal/tasks/load", json=lookup
                )
                response.raise_for_status()
                snapshot = Snapshot.model_validate(response.json())
                if task.stream is not None and snapshot.checkpoint >= task.events[-1].offset:
                    return {"status": "already_committed", "checkpoint": snapshot.checkpoint}
                change = prepare_commit(task, snapshot, settings.max_pending_measurements, strict)
                response = await app.state.client.post(
                    settings.data_url + "/internal/tasks/commit",
                    content=change.model_dump_json().encode(),
                    headers={"Content-Type": "application/json"},
                )
                if response.status_code == 409:
                    conflicts.inc()
                    continue
                response.raise_for_status()
                batches.inc()
                rejected.inc(len(change.rejected))
                if change.rejected:
                    log.warning(
                        "Quarantined %d events in partition %d",
                        len(change.rejected),
                        task.partition,
                    )
                return {"status": "committed", "checkpoint": change.checkpoint}
            raise HTTPException(503, "concurrent state updates; retry this batch")
        except httpx.HTTPError as exc:
            raise HTTPException(503, "data service unavailable; retry this batch") from exc
        finally:
            app.state.active -= 1
            active.dec()

    @app.post("/tasks")
    async def tasks(task: TaskBatch):
        if task.stream is None:
            raise HTTPException(422, "Kafka tasks require a stream identifier")
        return await execute(task)

    @app.post("/configure")
    async def configure(config: ExperimentConfig):
        task = TaskBatch(
            events=[
                EventEnvelope(offset=0, kind=EventType.CONFIGURED, records=[config.model_dump()])
            ]
        )
        await execute(task, strict=True)
        return {"experiment": config.experiment, "status": "accepted"}

    return app


app = create_app()
````

## temperature\_observability/healthcheck.py

Container probe entry point; readiness is distinct from business correctness under workload.

SHA256: `816b6299a37e39ee160362bb701aa0b0ba5ec438c83e9ffaecb8ee00f98aa61f`

````python
"""Small readiness probe: avoid importing the HTTP/TLS stack in every container."""

import socket


def main():
    with socket.create_connection(("127.0.0.1", 8000), timeout=2) as connection:
        connection.sendall(b"GET /health HTTP/1.0\r\nHost: localhost\r\n\r\n")
        with connection.makefile("rb") as response:
            status = response.readline(256).split()
            if len(status) < 2 or status[1] != b"200":
                raise SystemExit(1)


if __name__ == "__main__":
    main()
````

## temperature\_observability/history.py

Bounded versioned small-history snapshots, inclusive binary-search selection and interval rendering.

SHA256: `62a7150643e6a1886c982bb925579a0bc011fda5106ef73ad4c00d33dcca0f81`

````python
"""Bounded read snapshots for repeated queries over small experiment histories."""

import json
from bisect import bisect_left, bisect_right
from collections import OrderedDict
from dataclasses import dataclass


@dataclass(frozen=True)
class HistorySnapshot:
    version: int
    ended: float | None
    points: list[dict]

    def __post_init__(self):
        object.__setattr__(self, "timestamps", [point["timestamp"] for point in self.points])

    def render(self, start=None, end=None, outside=False, intervals=False):
        if intervals:
            result, active = [], None
            for point in self.points:
                if point["out_of_range"] and active is None:
                    active = {"start_time": point["timestamp"], "end_time": None}
                    result.append(active)
                elif not point["out_of_range"] and active is not None:
                    active["end_time"] = point["timestamp"]
                    active = None
            if active is not None and self.ended is not None:
                active["end_time"] = self.ended
        else:
            left = bisect_left(self.timestamps, start) if start is not None else 0
            right = bisect_right(self.timestamps, end) if end is not None else len(self.points)
            result = [
                {"timestamp": p["timestamp"], "temperature": p["temperature"]}
                for p in self.points[left:right]
                if not outside or p["out_of_range"]
            ]
        return json.dumps(result, separators=(",", ":"))


class HistoryCache:
    """Bound both point storage and entry count; large histories stay in SQL/Redis.

    None marks an experiment exceeding the per-experiment limit. Histories are
    append-only, so repeated narrow queries need not probe its first 513 rows
    again. Evicting a marker only causes another bounded probe, never data loss.
    """

    def __init__(self, max_points=16384, max_entries=256):
        self.max_points, self.max_entries = max_points, max_entries
        self.entries = OrderedDict()
        self.points = 0

    def get(self, experiment, version):
        snapshot = self.entries.get(experiment)
        if experiment in self.entries:
            self.entries.move_to_end(experiment)
        if snapshot is not None and snapshot.version >= version:
            return snapshot
        return None

    def is_large(self, experiment):
        return experiment in self.entries and self.entries[experiment] is None

    def put(self, experiment, snapshot):
        previous = self.entries.get(experiment)
        if previous is not None and snapshot is not None and previous.version > snapshot.version:
            return
        self.points -= len(previous.points) if previous is not None else 0
        self.entries[experiment] = snapshot
        self.entries.move_to_end(experiment)
        self.points += len(snapshot.points) if snapshot is not None else 0
        while self.points > self.max_points or len(self.entries) > self.max_entries:
            _, removed = self.entries.popitem(last=False)
            self.points -= len(removed.points) if removed is not None else 0
````

## temperature\_observability/models.py

Typed events, states, task lookups, commit proposals, partial groups, measurements and notification payloads.

SHA256: `6baa98d4398f7be2e432e5f6f80daa23546ca33c58f0ceca1175eb0f8e574588`

````python
from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, FiniteFloat, model_validator


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class EventType(StrEnum):
    CONFIGURED = "experiment_configured"
    STABILIZATION = "stabilization_started"
    STARTED = "experiment_started"
    MEASURED = "sensor_temperature_measured"
    TERMINATED = "experiment_terminated"


class TemperatureRange(Model):
    lower_threshold: FiniteFloat
    upper_threshold: FiniteFloat

    @model_validator(mode="after")
    def ordered(self):
        if self.lower_threshold > self.upper_threshold:
            raise ValueError("lower_threshold must be <= upper_threshold")
        return self


class ExperimentConfig(Model):
    experiment: str = Field(min_length=1, max_length=200)
    researcher: str = Field(min_length=3, max_length=320)
    sensors: list[str] = Field(min_length=1)
    temperature_range: TemperatureRange

    @model_validator(mode="after")
    def valid_sensors(self):
        if any(not s or len(s) > 200 for s in self.sensors):
            raise ValueError("sensor IDs must be nonempty and at most 200 characters")
        if len(set(self.sensors)) != len(self.sensors):
            raise ValueError("sensor IDs must be unique")
        if "@" not in self.researcher:
            raise ValueError("researcher must be an email address")
        return self


class LifecycleEvent(Model):
    experiment: str = Field(min_length=1, max_length=200)
    timestamp: FiniteFloat = Field(ge=0)


class SensorEvent(LifecycleEvent):
    sensor: str = Field(min_length=1, max_length=200)
    measurement_id: str = Field(min_length=1, max_length=200)
    temperature: FiniteFloat
    measurement_hash: str = Field(min_length=1)


class PartialMeasurement(Model):
    timestamp: float
    measurement_hash: str
    values: dict[str, float] = Field(default_factory=dict)


class ExperimentState(Model):
    config: ExperimentConfig
    status: Literal["configured", "stabilizing", "running", "terminated"] = "configured"
    phase_started_at: float | None = None
    started_at: float | None = None
    terminated_at: float | None = None
    previous_out_of_range: bool | None = None
    last_timestamp: float | None = None
    last_temperature: float | None = None
    pending: dict[str, PartialMeasurement] = Field(default_factory=dict)


class StoredState(Model):
    version: int
    state: ExperimentState


class EventEnvelope(Model):
    offset: int = Field(ge=0)
    kind: str
    records: list[dict[str, Any]] = Field(default_factory=list)
    error: str | None = None


class TaskBatch(Model):
    stream: str | None = None
    partition: int = Field(default=0, ge=0)
    events: list[EventEnvelope] = Field(min_length=1, max_length=1024)

    @model_validator(mode="after")
    def ordered(self):
        offsets = [e.offset for e in self.events]
        if offsets != sorted(set(offsets)):
            raise ValueError("task offsets must be unique and increasing")
        return self


class TaskLookup(Model):
    stream: str | None = None
    partition: int = Field(default=0, ge=0)
    experiments: list[str]
    measurements: list[tuple[str, str]]

    @classmethod
    def from_task(cls, task: TaskBatch):
        experiments, measurements = set(), set()
        for event in task.events:
            for record in event.records:
                experiment = record.get("experiment")
                if isinstance(experiment, str):
                    experiments.add(experiment)
                    measurement = record.get("measurement_id", record.get("measurement-id"))
                    if isinstance(measurement, str):
                        measurements.add((experiment, measurement))
        return cls(
            stream=task.stream,
            partition=task.partition,
            experiments=sorted(experiments),
            measurements=sorted(measurements),
        )


class Snapshot(Model):
    checkpoint: int = -1
    states: dict[str, StoredState] = Field(default_factory=dict)
    completed: list[tuple[str, str]] = Field(default_factory=list)


class Measurement(Model):
    experiment_id: str
    measurement_id: str
    timestamp: float
    temperature: float
    phase: str
    out_of_range: bool


class TemperaturePoint(Model):
    timestamp: FiniteFloat
    temperature: FiniteFloat


class Notification(Model):
    notification_type: Literal["Stabilized", "OutOfRange"]
    researcher: str
    experiment_id: str
    measurement_id: str
    cipher_data: str


class NotificationJob(Model):
    payload: Notification
    measured_at: float


class RejectedEvent(Model):
    offset: int
    kind: str
    reason: str
    records: list[dict[str, Any]]


class Commit(Model):
    stream: str | None
    partition: int
    expected_checkpoint: int
    checkpoint: int
    states: dict[str, StoredState]
    measurements: list[Measurement]
    notifications: list[NotificationJob]
    rejected: list[RejectedEvent]


class NotificationResult(Model):
    lease_token: str
    outcome: Literal["sent", "retry", "failed"]
    retry_after: float = Field(default=0, ge=0, le=3600)
    status_code: int | None = None
    latency: float | None = None


class ClaimRequest(Model):
    limit: int = Field(default=32, ge=1, le=256)
    lease_seconds: float = Field(default=15, ge=1, le=300)
````

## temperature\_observability/notification.py

Leased durable-job dispatch, token read, idempotency key, timeout/retry and individual deadline accounting.

SHA256: `e64d0d87c153af4c10d2d90999cb1d40db5170e2d4a353196e5b1356b57214ee`

````python
import asyncio
import logging
import time
from contextlib import asynccontextmanager
from pathlib import Path

import httpx
from fastapi import FastAPI, HTTPException

from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
from .config import Settings

log = logging.getLogger(__name__)


def retry_delay(attempt: int, retry_after: str | None = None) -> float:
    # Retry-After's numeric form is supported and capped; no unbounded exponential growth.
    try:
        if retry_after is not None:
            return max(0.1, min(float(retry_after), 60))
    except ValueError:
        pass
    return min(0.25 * 2 ** min(attempt - 1, 5), 5)


class NotificationHandler:
    def __init__(self, settings, client, metrics):
        self.settings, self.client = settings, client
        self.latency = metrics.histogram(
            "to_notification_latency_seconds", "Measurement to accepted notification"
        )
        self.results = metrics.counter(
            "to_notification_attempts_total", "Notification delivery outcomes", ["outcome"]
        )
        self.deadline_misses = metrics.counter(
            "to_notification_deadline_misses_total",
            "Accepted notifications taking at least 10 seconds",
        )
        self.last_successful_poll = time.monotonic()

    async def deliver(self, job):
        result = {"lease_token": job["lease_token"], "outcome": "retry"}
        try:
            # Re-read for credential rotation; never log the token or request URL.
            token = (
                (
                    await asyncio.to_thread(Path(self.settings.notification_token_file).read_text)
                ).strip()
                if self.settings.notification_token_file
                else None
            )
            response = await self.client.post(
                self.settings.notification_url,
                params={"token": token} if token else None,
                json=job["payload"],
                timeout=self.settings.notification_timeout,
                headers={
                    "Idempotency-Key": ":".join(
                        job["payload"][key]
                        for key in ("experiment_id", "measurement_id", "notification_type")
                    )
                },
            )
            result["status_code"] = response.status_code
            if 200 <= response.status_code < 300:
                result["outcome"] = "sent"
                result["latency"] = max(0, time.time() - job["measured_at"])
                self.latency.observe(result["latency"])
                if result["latency"] >= 10:
                    self.deadline_misses.inc()
            elif response.status_code in {400, 404, 405, 410, 413, 415, 422}:
                result["outcome"] = "failed"
                log.error(
                    "Notification job %s rejected with HTTP %s", job["id"], response.status_code
                )
            else:
                result["retry_after"] = retry_delay(
                    job["attempts"], response.headers.get("Retry-After")
                )
        except (httpx.HTTPError, OSError):
            result["retry_after"] = retry_delay(job["attempts"])
        self.results.labels(result["outcome"]).inc()
        # An ambiguous HTTP outcome can cause redelivery. The external API must honor
        # the idempotency key to eliminate the acceptance/acknowledgement crash window.
        try:
            response = await self.client.post(
                self.settings.data_url + f"/internal/notifications/{job['id']}/result",
                json=result,
            )
            response.raise_for_status()
        except httpx.HTTPError:
            log.warning(
                "Could not acknowledge notification job %s; its lease will expire", job["id"]
            )

    async def run(self):
        while True:
            try:
                response = await self.client.post(
                    self.settings.data_url + "/internal/notifications/claim",
                    json={
                        "limit": self.settings.notification_concurrency,
                        "lease_seconds": self.settings.notification_lease,
                    },
                )
                response.raise_for_status()
                self.last_successful_poll = time.monotonic()
                jobs = response.json()
                if jobs:
                    await asyncio.gather(*(self.deliver(job) for job in jobs))
                    continue  # Drain a backlog without adding another interval per batch.
            except httpx.HTTPError:
                log.warning("Notification outbox unavailable; retrying")
            await asyncio.sleep(self.settings.notification_interval)


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client() as client:
            handler = NotificationHandler(settings, client, metrics)
            app.state.handler = handler
            app.state.task = asyncio.create_task(supervise(handler.run, "notification dispatcher"))
            yield
            await cancel_tasks(app.state.task)

    app = FastAPI(title="Notification dispatcher (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        if app.state.task.done() or time.monotonic() - app.state.handler.last_successful_poll > 30:
            raise HTTPException(503, "notification dispatcher is not polling")
        return {"status": "ok"}

    return app


app = create_app()
````

## temperature\_observability/scheduler.py

Ordered partition dispatch, durable-before-Kafka acknowledgement, global/per-partition backpressure and status.

SHA256: `bec84a1e2c6111c46dc0073b564e26168f52bde36cb2824b8dd015b24634d90a`

````python
import asyncio
import logging
import time
from collections import deque
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from functools import partial

import httpx
from confluent_kafka import Consumer, KafkaException, TopicPartition
from fastapi import FastAPI, HTTPException

from .codec import decode_event
from .common import Metrics, cancel_tasks, configure_logging, http_client, supervise
from .config import Settings
from .discovery import ExecutorPool
from .models import TaskBatch

log = logging.getLogger(__name__)


class KafkaConsumerAndTaskScheduler:
    def __init__(self, settings: Settings, client: httpx.AsyncClient, metrics: Metrics):
        self.settings, self.client = settings, client
        self.executors = ExecutorPool(settings.executor_host, settings.executor_port, client)
        self.queues: dict[int, deque] = {}
        self.running: dict[int, asyncio.Task] = {}
        self.inflight: dict[int, int] = {}
        self.assigned: set[int] = set()
        self.paused = False
        self.paused_partitions: set[int] = set()
        self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
        self.received = metrics.counter("to_kafka_received_total", "Kafka messages consumed")
        self.committed = metrics.counter(
            "to_kafka_processed_total", "Durably processed Kafka messages"
        )
        self.failures = metrics.counter("to_scheduler_retries_total", "Batch dispatch retries")
        self.lag = metrics.histogram("to_kafka_event_age_seconds", "Kafka message age on receipt")
        self.backlog = metrics.gauge("to_scheduler_queued_events", "Queued and in-flight events")
        self.workers = metrics.gauge("to_scheduler_executors", "Healthy discovered executors")
        self.paused_count = metrics.gauge(
            "to_scheduler_paused_partitions", "Partitions paused by bounded backpressure"
        )

    async def kafka_call(self, method, *args, **kwargs):
        return await asyncio.get_running_loop().run_in_executor(
            self.thread, partial(method, *args, **kwargs)
        )

    def on_assign(self, partitions):
        self.assigned = set(partitions)
        for partition in partitions:
            self.queues.setdefault(partition, deque())
        self.paused = False
        self.paused_partitions.clear()

    def on_revoke(self, partitions):
        for partition in partitions:
            self.assigned.discard(partition)
            self.paused_partitions.discard(partition)
            self.queues.pop(partition, None)
            task = self.running.pop(partition, None)
            if task:
                task.cancel()
            self.inflight.pop(partition, None)

    async def discover(self):
        while True:
            await self.executors.refresh()
            self.workers.set(len(self.executors.workers))
            await asyncio.sleep(0.5)

    async def dispatch_partition(self, partition):
        queue = self.queues[partition]
        while queue:
            batch = [queue.popleft() for _ in range(min(len(queue), self.settings.batch_size))]
            self.inflight[partition] = len(batch)
            task = TaskBatch(
                stream=f"{self.settings.kafka_group}/{self.settings.kafka_topic}",
                partition=partition,
                events=batch,
            )
            body = task.model_dump_json().encode()
            while True:
                worker = self.executors.reserve()
                if worker is None:
                    await asyncio.sleep(0.025)
                    continue
                success = False
                try:
                    response = await self.client.post(
                        worker.url + "/tasks",
                        content=body,
                        headers={"Content-Type": "application/json"},
                    )
                    response.raise_for_status()
                    success = True
                except httpx.HTTPError:
                    self.failures.inc()
                finally:
                    self.executors.release(worker)
                if success:
                    break
                await asyncio.sleep(0.1)
            # Only durable Data commits authorize Kafka commits. Replaying a batch is safe.
            try:
                await self.kafka_call(
                    self.consumer.commit,
                    offsets=[
                        TopicPartition(self.settings.kafka_topic, partition, batch[-1].offset + 1)
                    ],
                    asynchronous=False,
                )
            except KafkaException:
                # Ownership may have moved. A new owner can replay from PostgreSQL's checkpoint.
                log.warning(
                    "Kafka offset commit failed for partition %d; durable state retained", partition
                )
            self.committed.inc(len(batch))
            self.inflight.pop(partition, None)

    def status(self):
        return {
            "queued_events": sum(len(q) for q in self.queues.values())
            + sum(self.inflight.values()),
            "assigned_partitions": len(self.assigned),
            "executors": len(self.executors.workers),
            "active_tasks": sum(w.load for w in self.executors.workers.values()),
            "capacity": sum(w.capacity for w in self.executors.workers.values()),
            "paused_partitions": sorted(self.paused_partitions),
            "partition_queues": {
                str(p): len(q) + self.inflight.get(p, 0) for p, q in self.queues.items()
            },
        }

    async def apply_backpressure(self):
        total = self.status()["queued_events"]
        self.backlog.set(total)
        if total >= self.settings.queue_capacity:
            self.paused = True
        elif total < self.settings.queue_capacity // 2:
            self.paused = False
        # A hot partition must not consume the entire queue budget and pause
        # every other partition while most executors are idle. Keep the global
        # cap as a final guard, and apply per-partition high/low watermarks too.
        share = max(
            self.settings.batch_size, self.settings.queue_capacity // max(1, len(self.assigned))
        )
        pause, resume = set(), set()
        for partition in self.assigned:
            queued = len(self.queues.get(partition, ())) + self.inflight.get(partition, 0)
            if self.paused or queued >= share:
                if partition not in self.paused_partitions:
                    pause.add(partition)
            elif queued < share // 2 and partition in self.paused_partitions:
                resume.add(partition)
        if pause:
            await self.kafka_call(
                self.consumer.pause,
                [TopicPartition(self.settings.kafka_topic, p) for p in sorted(pause)],
            )
            self.paused_partitions.update(pause)
        if resume:
            await self.kafka_call(
                self.consumer.resume,
                [TopicPartition(self.settings.kafka_topic, p) for p in sorted(resume)],
            )
            self.paused_partitions.difference_update(resume)
        self.paused_count.set(len(self.paused_partitions))

    async def run(self):
        loop = asyncio.get_running_loop()
        self.consumer = Consumer(
            {
                **self.settings.kafka_config(),
                "group.id": self.settings.kafka_group,
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
                "enable.auto.offset.store": False,
                "fetch.wait.max.ms": 50,
                "queued.max.messages.kbytes": 16384,
            }
        )
        self.consumer.subscribe(
            [self.settings.kafka_topic],
            on_assign=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_assign, [p.partition for p in ps]
            ),
            on_revoke=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_revoke, [p.partition for p in ps]
            ),
            on_lost=lambda _c, ps: loop.call_soon_threadsafe(
                self.on_revoke, [p.partition for p in ps]
            ),
        )
        discovery = asyncio.create_task(self.discover())
        try:
            while True:
                await self.apply_backpressure()
                messages = await self.kafka_call(
                    self.consumer.consume, num_messages=512, timeout=0.05
                )
                for message in messages:
                    if message.error():
                        log.warning("Kafka returned error code %s", message.error().code())
                        continue
                    if message.partition() not in self.assigned:
                        continue
                    self.received.inc()
                    timestamp = message.timestamp()[1]
                    if timestamp > 0:
                        self.lag.observe(max(0, time.time() - timestamp / 1000))
                    self.queues[message.partition()].append(
                        decode_event(message.offset(), message.headers(), message.value())
                    )
                for partition, queue in list(self.queues.items()):
                    current = self.running.get(partition)
                    if current and current.done():
                        # Unexpected programming errors must fail the service, not lose a popped batch.
                        current.result()
                        self.running.pop(partition, None)
                        current = None
                    if queue and current is None:
                        self.running[partition] = asyncio.create_task(
                            self.dispatch_partition(partition)
                        )
        finally:
            await cancel_tasks(discovery, *self.running.values())
            await self.kafka_call(self.consumer.close)
            self.thread.shutdown(wait=False)
            self.thread = ThreadPoolExecutor(max_workers=1, thread_name_prefix="kafka")
            self.queues.clear()
            self.running.clear()
            self.inflight.clear()
            self.assigned.clear()
            self.paused = False
            self.paused_partitions.clear()


def create_app(settings: Settings | None = None):
    settings = settings or Settings()
    metrics = Metrics()

    @asynccontextmanager
    async def lifespan(app):
        configure_logging()
        async with http_client(timeout=20) as client:
            scheduler = KafkaConsumerAndTaskScheduler(settings, client, metrics)
            app.state.scheduler = scheduler
            app.state.task = asyncio.create_task(supervise(scheduler.run, "Kafka scheduler"))
            yield
            await cancel_tasks(app.state.task)

    app = FastAPI(title="Kafka consumer and scheduler (internal)", lifespan=lifespan)
    metrics.install(app)

    @app.get("/health")
    async def health():
        if app.state.task.done() or not app.state.scheduler.assigned:
            raise HTTPException(503, "Kafka consumer is not assigned or has stopped")
        return {"status": "ok"}

    @app.get("/status")
    async def status():
        return app.state.scheduler.status()

    return app


app = create_app()
````

## temperature\_observability/schema.sql

Application tables/constraints and query/outbox indexes; PostgreSQL is the source of truth.

SHA256: `8ce9064d2d5cb4a47bfe6e959b15ffa6247f53fc149e5e7f7d50cf1ac6f38c1c`

````sql
CREATE TABLE IF NOT EXISTS experiments (
    id TEXT PRIMARY KEY,
    version BIGINT NOT NULL,
    state JSONB NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS checkpoints (
    stream TEXT NOT NULL,
    partition INTEGER NOT NULL,
    last_offset BIGINT NOT NULL,
    PRIMARY KEY (stream, partition)
);

CREATE TABLE IF NOT EXISTS measurements (
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    measurement_id TEXT NOT NULL,
    timestamp DOUBLE PRECISION NOT NULL,
    temperature DOUBLE PRECISION NOT NULL,
    phase TEXT NOT NULL CHECK (phase IN ('stabilizing', 'running')),
    out_of_range BOOLEAN NOT NULL,
    PRIMARY KEY (experiment_id, measurement_id)
);
CREATE INDEX IF NOT EXISTS measurements_history
    ON measurements (experiment_id, timestamp, measurement_id) WHERE phase = 'running';
CREATE INDEX IF NOT EXISTS measurements_out_of_range
    ON measurements (experiment_id, timestamp, measurement_id)
    WHERE phase = 'running' AND out_of_range;

CREATE TABLE IF NOT EXISTS notification_outbox (
    id BIGSERIAL PRIMARY KEY,
    experiment_id TEXT NOT NULL REFERENCES experiments(id),
    measurement_id TEXT NOT NULL,
    notification_type TEXT NOT NULL,
    payload JSONB NOT NULL,
    measured_at DOUBLE PRECISION NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'sent', 'failed')),
    attempts INTEGER NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    lease_until TIMESTAMPTZ,
    lease_token TEXT,
    delivered_at TIMESTAMPTZ,
    latency DOUBLE PRECISION,
    last_status INTEGER,
    UNIQUE (experiment_id, measurement_id, notification_type)
);
CREATE INDEX IF NOT EXISTS notification_pending
    ON notification_outbox (next_attempt_at, id) WHERE status = 'pending';
-- Keep the monitoring count off the large payload heap as history accumulates.
CREATE INDEX IF NOT EXISTS notification_status ON notification_outbox (status);

CREATE TABLE IF NOT EXISTS rejected_events (
    stream TEXT NOT NULL,
    partition INTEGER NOT NULL,
    offset_id BIGINT NOT NULL,
    kind TEXT NOT NULL,
    reason TEXT NOT NULL,
    records JSONB NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (stream, partition, offset_id)
);
````

## tests/test\_codec.py

Avro decoding, termination header compatibility and invalid-message behavior.

SHA256: `d69ab8f668e9df3d5ab8026db4695f25061a9d49e0ffb95f4dc2a7ae2c2020e1`

````python
import io
import json
from pathlib import Path

import fastavro
import pytest

from temperature_observability.codec import decode_event


def test_course_termination_header_overrides_embedded_schema():
    schema = json.loads(Path("experiment-producer/schemas/experiment_started.avsc").read_text())
    body = {"experiment": "exp", "timestamp": 10.5}
    data = io.BytesIO()
    fastavro.writer(data, schema, [body])
    decoded = decode_event(42, [("record_name", b"experiment_terminated")], data.getvalue())
    assert decoded.kind == "experiment_terminated"
    assert decoded.records == [body] and decoded.error is None


@pytest.mark.parametrize(
    "headers,body",
    [
        (None, b"bad"),
        ([("record_name", b"unknown")], b"Obj\x01"),
        ([("record_name", b"experiment_started")], None),
        ([("record_name", b"experiment_started")], b"Obj\x01truncated"),
        ([("record_name", b"\xff")], b"data"),
    ],
)
def test_bad_avro_is_quarantined(headers, body):
    result = decode_event(1, headers, body)
    assert result.error and result.offset == 1


def test_non_finite_avro_cannot_poison_jsonb_quarantine():
    schema = json.loads(
        Path("experiment-producer/schemas/sensor_temperature_measured.avsc").read_text()
    )
    data = io.BytesIO()
    fastavro.writer(
        data,
        schema,
        [
            {
                "experiment": "e",
                "sensor": "s",
                "measurement_id": "m",
                "timestamp": 1,
                "temperature": float("nan"),
                "measurement_hash": "opaque",
            }
        ],
    )
    event = decode_event(1, [("record_name", b"sensor_temperature_measured")], data.getvalue())
    assert event.error and not event.records
````

## tests/test\_cost.py

Provisioned-storage billing estimate and incomplete-configuration behavior.

SHA256: `a1b5891c055a60d3c370b4df4ac65c13bc88fcfd8f7c0f9406c1631c3797d038`

````python
import pytest

from temperature_observability.config import Settings


def test_cost_uses_provisioned_capacity_and_one_shared_vm():
    settings = Settings(
        vm_hourly_cost_usd=0.0408,
        storage_provisioned_gb=20,
        storage_gb_monthly_cost_usd=0.088,
        executor_replicas=4,
    )
    assert settings.cost_configured
    assert settings.estimated_hourly_cost_usd * 730 == pytest.approx(0.0408 * 730 + 20 * 0.088)


def test_missing_storage_inputs_are_not_a_fully_configured_cost():
    assert not Settings(vm_hourly_cost_usd=0.0408).cost_configured
````

## tests/test\_course\_build\_cache.py

Source/image fingerprint invalidation, including schemas and SQL metadata while excluding credentials.

SHA256: `d805c123f786449f419c933cd213e0ca3a048e38516a371bb993297fec429a07`

````python
import pytest

from loadtests.course import course_tools_fingerprint


@pytest.fixture
def sources(tmp_path):
    files = {
        "Cargo.toml": '[workspace]\nmembers=["demo"]\n',
        "Cargo.lock": "locked versions",
        ".dockerignore": "source allowlist",
        "deploy/CourseTools.Dockerfile": "FROM rust",
        "deploy/CourseTools.Dockerfile.dockerignore": "source allowlist",
        "notifications-service/ca.crt": "public certificate",
        "demo/Cargo.toml": "[package]",
        "demo/src/lib.rs": "fn example() {}",
        ".sqlx/query.json": "{}",
        "experiment-producer/schemas/measurement.avsc": "{}",
    }
    for name, text in files.items():
        path = tmp_path / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(text)
    return tmp_path


def test_private_files_and_application_changes_do_not_rebuild_instructor_tools(sources):
    before = course_tools_fingerprint(sources)
    for name in (".env", "demo/auth/private.key", "temperature_observability/data.py"):
        path = sources / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text("unrelated or private contents")
    assert course_tools_fingerprint(sources) == before


@pytest.mark.parametrize(
    "name",
    [
        "Cargo.lock",
        "demo/src/lib.rs",
        ".sqlx/query.json",
        "experiment-producer/schemas/measurement.avsc",
    ],
)
def test_changed_public_inputs_invalidate_the_compiled_tools(sources, name):
    before = course_tools_fingerprint(sources)
    path = sources / name
    path.write_text(path.read_text() + "changed")
    assert course_tools_fingerprint(sources) != before
````

## tests/test\_data\_integration.py

Real PostgreSQL atomicity/version rollback, leases, replay, cache coherence, delayed writers and large-history fallback.

SHA256: `430e727998b07c5407fc9380f2fba4ce73330a3ce9ed83a20a13153f62ff2d59`

````python
import asyncio
import os
from uuid import uuid4

import asyncpg
import httpx
import pytest

from temperature_observability.config import Settings
from temperature_observability.data import create_app
from temperature_observability.executor import prepare_commit
from temperature_observability.models import EventEnvelope, Snapshot, TaskBatch, TaskLookup

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TO_TEST_STACK") != "1",
        reason="set TO_TEST_STACK=1 with local Compose running",
    ),
]


@pytest.fixture
async def data_client(request):
    admin_url = os.environ.get(
        "TO_TEST_DATABASE_URL",
        "postgresql://observability:observability@localhost:15432/observability",
    )
    database = "test_" + uuid4().hex
    admin = await asyncpg.connect(admin_url)
    await admin.execute(f'CREATE DATABASE "{database}"')
    settings = Settings(
        database_url=admin_url.rsplit("/", 1)[0] + "/" + database,
        # Deliberately unavailable: PostgreSQL must remain a correct fallback.
        redis_url="redis://127.0.0.1:1/0",
        **getattr(request, "param", {}),
    )
    app = create_app(settings)
    try:
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://data"
            ) as client:
                yield client, app
    finally:
        await admin.execute(f'DROP DATABASE "{database}" WITH (FORCE)')
        await admin.close()


def config(experiment="e"):
    return {
        "experiment": experiment,
        "researcher": "r@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }


async def process(client, records, start=0):
    task = TaskBatch(
        stream="test-stream",
        events=[
            EventEnvelope(offset=i + start, kind=kind, records=[payload])
            for i, (kind, payload) in enumerate(records)
        ],
    )
    response = await client.post(
        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
    )
    response.raise_for_status()
    snapshot = Snapshot.model_validate(response.json())
    change = prepare_commit(task, snapshot, 256)
    response = await client.post("/internal/tasks/commit", json=change.model_dump())
    response.raise_for_status()
    return task, change


def measurement(sensor, value, mid, timestamp):
    return (
        "sensor_temperature_measured",
        {
            "experiment": "e",
            "sensor": sensor,
            "temperature": value,
            "timestamp": timestamp,
            "measurement_id": mid,
            "measurement_hash": "opaque",
        },
    )


async def test_atomic_state_history_outbox_and_replay(data_client):
    client, app = data_client
    task, change = await process(
        client,
        [
            ("experiment_configured", config()),
            ("stabilization_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 26, "stabilized", 2),
            measurement("b", 25, "stabilized", 2),
            ("experiment_started", {"experiment": "e", "timestamp": 3}),
            measurement("b", 28, "outside", 4),
            measurement("a", 26, "outside", 4),
            measurement("a", 26, "still-out", 5),
            measurement("b", 28, "still-out", 5),
            measurement("a", 25, "normal", 6),
            measurement("b", 26, "normal", 6),
            ("experiment_terminated", {"experiment": "e", "timestamp": 7}),
        ],
    )
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 409
    response = await client.post(
        "/internal/tasks/load", json=TaskLookup.from_task(task).model_dump()
    )
    replay = prepare_commit(task, Snapshot.model_validate(response.json()), 256)
    assert not replay.states and not replay.notifications and not replay.measurements
    history = await client.get(
        "/temperature", params={"experiment-id": "e", "start-time": 0, "end-time": 9}
    )
    assert history.json() == [
        {"timestamp": 4, "temperature": 27},
        {"timestamp": 5, "temperature": 27},
        {"timestamp": 6, "temperature": 25.5},
    ]
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
    ).json() == history.json()[:2]
    assert (
        await client.get("/temperature/out-of-range/intervals", params={"experiment-id": "e"})
    ).json() == [{"start_time": 4, "end_time": 6}]
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 2
        assert await conn.fetchval("SELECT last_offset FROM checkpoints") == 11


async def test_cas_failure_rolls_back_earlier_writes(data_client):
    client, app = data_client
    _, change = await process(client, [("experiment_configured", config("z-existing"))])
    change.stream = None
    change.states["z-existing"].version = 999
    new_state = change.states["z-existing"].model_copy(deep=True)
    new_state.version = -1
    new_state.state.config.experiment = "a-new"
    change.states["a-new"] = new_state
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 409
    assert (await client.get("/experiments/a-new")).status_code == 404
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM experiments") == 1


async def test_pool_release_rolls_back_an_unfinished_transaction(data_client):
    client, app = data_client
    await process(client, [("experiment_configured", config())])
    async with app.state.pool.acquire() as conn:
        await conn.execute("BEGIN")
        await conn.execute("UPDATE experiments SET version=99")
    async with app.state.pool.acquire() as conn:
        assert not conn.is_in_transaction()
        assert await conn.fetchval("SELECT version FROM experiments WHERE id='e'") == 0


async def test_batched_versions_are_all_checked_and_conflicts_roll_back(data_client):
    client, app = data_client
    _, change = await process(
        client, [("experiment_configured", config(name)) for name in ("a", "b", "c")]
    )
    change.stream = None
    for saved in change.states.values():
        saved.version = 0
        saved.state.status = "running"
    change.states["b"].version = 999
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 409
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
    assert len(rows) == 3
    assert all(row["version"] == 0 and row["status"] == "configured" for row in rows)
    change.states["b"].version = 0
    assert (
        await client.post("/internal/tasks/commit", json=change.model_dump())
    ).status_code == 200
    async with app.state.pool.acquire() as conn:
        rows = await conn.fetch("SELECT version,state->>'status' AS status FROM experiments")
    assert all(row["version"] == 1 and row["status"] == "running" for row in rows)


async def test_partial_sensors_survive_reload_and_duplicate_new_offset(data_client):
    client, app = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 27, "m", 2),
        ],
    )
    assert (await client.get("/experiments/e")).json()["pending_measurements"] == 1
    await process(client, [measurement("b", 27, "m", 2)], start=3)
    await process(client, [measurement("b", 27, "m", 2), measurement("a", 27, "m", 2)], start=4)
    async with app.state.pool.acquire() as conn:
        assert await conn.fetchval("SELECT count(*) FROM measurements") == 1
        assert await conn.fetchval("SELECT count(*) FROM notification_outbox") == 1


async def test_claims_are_exclusive_and_acknowledgements_are_fenced(data_client):
    client, _ = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 27, "m", 2),
            measurement("b", 27, "m", 2),
        ],
    )
    first = (
        await client.post("/internal/notifications/claim", json={"limit": 1, "lease_seconds": 15})
    ).json()
    assert len(first) == 1
    assert (await client.post("/internal/notifications/claim", json={})).json() == []
    path = f"/internal/notifications/{first[0]['id']}/result"
    assert (
        await client.post(path, json={"lease_token": "wrong", "outcome": "sent"})
    ).status_code == 409
    assert (
        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "retry"})
    ).status_code == 200
    second = (await client.post("/internal/notifications/claim", json={})).json()[0]
    assert second["attempts"] == 2 and second["lease_token"] != first[0]["lease_token"]
    assert (
        await client.post(path, json={"lease_token": first[0]["lease_token"], "outcome": "sent"})
    ).status_code == 409


async def test_query_validation_and_empty_configured_experiment(data_client):
    client, _ = data_client
    await process(client, [("experiment_configured", config())])
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
    ).json() == []
    assert (
        await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
    ).status_code == 404
    for start, end in [(5, 1), ("nan", 1), (1, "inf")]:
        response = await client.get(
            "/temperature", params={"experiment-id": "e", "start-time": start, "end-time": end}
        )
        assert response.status_code == 422


@pytest.mark.parametrize("data_client", [{"history_cache_points": 0}], indirect=True)
async def test_cached_history_cannot_hide_a_committed_measurement(data_client):
    client, app = data_client

    class Cache:
        def __init__(self):
            self.values = {}
            self.hits = 0

        async def get(self, key):
            result = self.values.get(key)
            self.hits += int(result is not None)
            return result

        async def set(self, key, value, **_):
            self.values[key] = value

    cache, original = Cache(), app.state.cache
    app.state.cache = cache
    try:
        await process(
            client,
            [
                ("experiment_configured", config()),
                ("experiment_started", {"experiment": "e", "timestamp": 1}),
            ],
        )
        params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
        assert (await client.get("/temperature", params=params)).json() == []
        assert (await client.get("/temperature", params=params)).json() == []
        assert cache.hits == 1
        await process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
        assert (await client.get("/temperature", params=params)).json() == [
            {"timestamp": 2, "temperature": 25.5}
        ]
    finally:
        app.state.cache = original


async def test_query_during_commit_cannot_repopulate_stale_metadata(data_client):
    client, app = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
        ],
    )
    params = {"experiment-id": "e", "start-time": 0, "end-time": 10}
    assert (await client.get("/temperature", params=params)).json() == []
    async with app.state.pool.acquire() as conn, conn.transaction():
        await conn.fetchval("SELECT id FROM experiments WHERE id='e' FOR UPDATE")
        writer = asyncio.create_task(
            process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
        )
        async with asyncio.timeout(5):
            while not app.state.metadata_writers["e"]:  # noqa: ASYNC110 - observe a real DB writer
                await asyncio.sleep(0.01)
        assert (await client.get("/temperature", params=params)).json() == []
        assert "e" not in app.state.history_metadata
    await writer
    assert (await client.get("/temperature", params=params)).json() == [
        {"timestamp": 2, "temperature": 25.5}
    ]


async def test_successful_commit_warms_metadata_without_an_extra_read(data_client):
    client, app = data_client
    await process(client, [("experiment_configured", config())])
    assert app.state.history_metadata["e"] == {"version": 0, "ended": None, "latest": None}
    response = await client.get("/temperature/out-of-range", params={"experiment-id": "e"})
    assert response.json() == []
    metrics = (await client.get("/metrics")).text
    assert 'operation="history_metadata"' not in metrics


async def test_delayed_old_writer_cannot_warm_over_a_newer_commit(data_client):
    client, app = data_client
    await process(client, [("experiment_configured", config())])
    released, resume = asyncio.Event(), asyncio.Event()
    original = app.state.pool

    class DelayedRelease:
        def __init__(self):
            self.first = True

        def __getattr__(self, name):
            return getattr(original, name)

        def acquire(self):
            from contextlib import asynccontextmanager

            @asynccontextmanager
            async def acquire():
                # Delay after the first writer's COMMIT and connection release.
                # Another writer can now commit while the first is still fencing.
                first, self.first = self.first, False
                async with original.acquire() as conn:
                    yield conn
                if first:
                    released.set()
                    await resume.wait()

            return acquire()

    task = TaskBatch(
        stream="test-stream",
        events=[
            EventEnvelope(
                offset=1,
                kind="experiment_started",
                records=[{"experiment": "e", "timestamp": 1}],
            )
        ],
    )
    snapshot = Snapshot.model_validate(
        (
            await client.post(
                "/internal/tasks/load",
                json=TaskLookup.from_task(task).model_dump(),
            )
        ).json()
    )
    change = prepare_commit(task, snapshot, 256)
    app.state.pool = DelayedRelease()
    writer = asyncio.create_task(client.post("/internal/tasks/commit", json=change.model_dump()))
    try:
        await asyncio.wait_for(released.wait(), 5)
        await process(client, [measurement("a", 25, "m", 2), measurement("b", 26, "m", 2)], start=2)
        resume.set()
        assert (await writer).status_code == 200
        assert (
            await client.get(
                "/temperature",
                params={
                    "experiment-id": "e",
                    "start-time": 0,
                    "end-time": 10,
                },
            )
        ).json() == [{"timestamp": 2, "temperature": 25.5}]
        assert app.state.history_metadata["e"]["version"] == 2
        assert not app.state.metadata_generations
    finally:
        resume.set()
        await writer
        app.state.pool = original


async def test_simultaneous_cold_reads_share_one_bounded_snapshot(data_client):
    client, app = data_client
    await process(client, [("experiment_configured", config())])
    responses = await asyncio.gather(
        *(
            client.get(
                "/temperature", params={"experiment-id": "e", "start-time": i, "end-time": i + 1}
            )
            for i in range(20)
        )
    )
    assert all(response.json() == [] for response in responses)
    metrics = (await client.get("/metrics")).text
    assert 'to_database_connection_seconds_count{operation="history_snapshot"} 1.0' in metrics
    assert not app.state.snapshot_locks


@pytest.mark.parametrize("data_client", [{"history_snapshot_limit": 2}], indirect=True)
async def test_large_history_falls_back_without_truncation_or_repeated_probes(data_client):
    client, app = data_client
    await process(
        client,
        [
            ("experiment_configured", config()),
            ("experiment_started", {"experiment": "e", "timestamp": 1}),
            measurement("a", 27, "a", 2),
            measurement("b", 27, "a", 2),
            measurement("a", 25, "b", 3),
            measurement("b", 26, "b", 3),
            measurement("a", 27, "c", 4),
            measurement("b", 27, "c", 4),
        ],
    )
    assert (
        await client.get(
            "/temperature",
            params={
                "experiment-id": "e",
                "start-time": 4,
                "end-time": 4,
            },
        )
    ).json() == [{"timestamp": 4, "temperature": 27}]
    assert app.state.histories.is_large("e")
    assert (
        len((await client.get("/temperature/out-of-range", params={"experiment-id": "e"})).json())
        == 2
    )
    metrics = (await client.get("/metrics")).text
    assert 'to_database_connection_seconds_count{operation="history_snapshot"} 1.0' in metrics
````

## tests/test\_engine.py

Domain means, lifecycle, allocated sensors, incomplete groups, duplicates and excursion semantics.

SHA256: `4a8c8e2dc8996a8ebba48abc9e02335ca7b8a9723556eccc856e27a6e0092735`

````python
import math

import pytest
from pydantic import ValidationError

from temperature_observability.engine import ExperimentStateManager, InvalidEvent
from temperature_observability.executor import prepare_commit
from temperature_observability.models import (
    EventEnvelope,
    ExperimentConfig,
    Snapshot,
    StoredState,
    TaskBatch,
)


def configuration(experiment="experiment"):
    return {
        "experiment": experiment,
        "researcher": "scientist@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }


def sensor(sensor_id="a", temperature=25.5, measurement="m1", timestamp=2):
    return {
        "experiment": "experiment",
        "sensor": sensor_id,
        "temperature": temperature,
        "timestamp": timestamp,
        "measurement_id": measurement,
        "measurement_hash": "opaque-cipher",
    }


def configured(phase="stabilization_started"):
    manager = ExperimentStateManager()
    state, _, _ = manager.apply(None, "experiment_configured", configuration(), set())
    if phase:
        state, _, _ = manager.apply(
            state, phase, {"experiment": "experiment", "timestamp": 1}, set()
        )
    return manager, state


def measure(manager, state, temperature, measurement="m1", timestamp=2):
    state, measurements, notifications = manager.apply(
        state,
        "sensor_temperature_measured",
        sensor("b", temperature - 1, measurement, timestamp),
        set(),
    )
    assert not measurements and not notifications
    return manager.apply(
        state,
        "sensor_temperature_measured",
        sensor("a", temperature + 1, measurement, timestamp),
        set(),
    )


def test_waits_for_all_sensors_and_uses_average():
    manager, state = configured()
    state, measurements, notifications = measure(manager, state, 25.5)
    assert measurements[0].temperature == 25.5
    assert measurements[0].phase == "stabilizing"
    assert notifications[0].payload.notification_type == "Stabilized"
    assert notifications[0].payload.cipher_data == "opaque-cipher"
    assert not state.pending


@pytest.mark.parametrize(
    "temperature,outside", [(25, False), (26, False), (24.99, True), (26.01, True)]
)
def test_range_is_inclusive(temperature, outside):
    manager, state = configured("experiment_started")
    _, measurements, notifications = measure(manager, state, temperature)
    assert measurements[0].out_of_range == outside
    assert bool(notifications) == outside


def test_notifies_on_each_excursion_only_once():
    manager, state = configured("experiment_started")
    alerts = []
    for i, temperature in enumerate([27, 28, 29, 25.5, 24, 23, 26, 27]):
        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
        alerts.extend(n.payload.measurement_id for n in notifications)
    assert alerts == ["0", "4", "7"]


def test_stabilization_reentry_matches_course_producer():
    manager, state = configured()
    alerts = []
    for i, temperature in enumerate([20, 25, 26, 27, 25.5]):
        state, _, notifications = measure(manager, state, temperature, str(i), i + 2)
        alerts.extend(n.payload.measurement_id for n in notifications)
    assert alerts == ["1", "4"]


def test_start_resets_stabilization_excursion_state():
    manager, state = configured()
    state, _, _ = measure(manager, state, 20)
    state, _, _ = manager.apply(
        state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
    )
    _, _, notifications = measure(manager, state, 20, "running", 4)
    assert notifications[0].payload.notification_type == "OutOfRange"


def test_duplicate_sensor_does_not_complete_group():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    state, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor(), set()
    )
    assert len(state.pending["m1"].values) == 1
    assert not measurements and not notifications


def test_conflicting_sensor_does_not_mutate_original():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="conflicting duplicate"):
        manager.apply(state, "sensor_temperature_measured", sensor(temperature=30), set())
    assert state.pending["m1"].values == {"a": 25.5}


def test_completed_group_replay_is_ignored_even_after_termination():
    manager, state = configured("experiment_started")
    state, _, _ = measure(manager, state, 27)
    state, _, _ = manager.apply(
        state, "experiment_terminated", {"experiment": "experiment", "timestamp": 3}, set()
    )
    _, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor(), {("experiment", "m1")}
    )
    assert not measurements and not notifications
    with pytest.raises(InvalidEvent, match="terminated"):
        manager.apply(
            state, "sensor_temperature_measured", sensor(measurement="new", timestamp=4), set()
        )


def test_interleaved_groups_are_completed_in_timestamp_order():
    manager, state = configured("experiment_started")
    readings = [
        sensor("a", 27, "first", 2),
        sensor("a", 25, "second", 3),
        sensor("b", 25, "second", 3),
    ]
    for reading in readings:
        state, measurements, _ = manager.apply(state, "sensor_temperature_measured", reading, set())
        assert not measurements
    state, measurements, notifications = manager.apply(
        state, "sensor_temperature_measured", sensor("b", 27, "first", 2), set()
    )
    assert [m.measurement_id for m in measurements] == ["first", "second"]
    assert [n.payload.measurement_id for n in notifications] == ["first"]
    assert state.previous_out_of_range is False


@pytest.mark.parametrize(
    "field,value", [("sensor", "alien"), ("timestamp", 0.5), ("temperature", math.inf)]
)
def test_invalid_readings_are_rejected(field, value):
    manager, state = configured()
    payload = sensor()
    payload[field] = value
    with pytest.raises((InvalidEvent, ValidationError)):
        manager.apply(state, "sensor_temperature_measured", payload, set())


def test_pending_groups_are_bounded_and_never_silently_evicted():
    manager, state = configured()
    manager.max_pending = 1
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="too many incomplete"):
        manager.apply(
            state, "sensor_temperature_measured", sensor(measurement="m2", timestamp=3), set()
        )
    assert list(state.pending) == ["m1"]


@pytest.mark.parametrize(
    "change",
    [
        {"sensors": []},
        {"sensors": ["a", "a"]},
        {"temperature_range": {"lower_threshold": 30, "upper_threshold": 20}},
        {"temperature_range": {"lower_threshold": float("nan"), "upper_threshold": 20}},
    ],
)
def test_configuration_validation(change):
    with pytest.raises(ValidationError):
        ExperimentConfig.model_validate({**configuration(), **change})


def test_batch_replay_skips_all_processed_offsets():
    _, state = configured()
    task = TaskBatch(
        stream="topic",
        events=[EventEnvelope(offset=4, kind="sensor_temperature_measured", records=[sensor()])],
    )
    change = prepare_commit(
        task,
        Snapshot(checkpoint=4, states={"experiment": StoredState(version=1, state=state)}),
        256,
    )
    assert not change.states and not change.measurements and not change.notifications


def test_poison_event_is_quarantined_without_blocking_later_configuration():
    task = TaskBatch(
        stream="topic",
        events=[
            EventEnvelope(offset=0, kind="unknown", error="invalid container"),
            EventEnvelope(offset=1, kind="experiment_configured", records=[configuration()]),
        ],
    )
    change = prepare_commit(task, Snapshot(), 256)
    assert change.checkpoint == 1 and change.states["experiment"].state.status == "configured"
    assert len(change.rejected) == 1 and change.rejected[0].offset == 0


def test_phase_transition_does_not_discard_incomplete_measurements():
    manager, state = configured()
    state, _, _ = manager.apply(state, "sensor_temperature_measured", sensor(), set())
    with pytest.raises(InvalidEvent, match="incomplete"):
        manager.apply(
            state, "experiment_started", {"experiment": "experiment", "timestamp": 3}, set()
        )
````

## tests/test\_history.py

Inclusive bounds, duplicate timestamps, version rejection, interval closure and entry/point eviction limits.

SHA256: `0306375a13e307819c1b8fb41fe0d4a68f2f05257dedd57a6d1d08c80320e230`

````python
import json

from temperature_observability.history import HistoryCache, HistorySnapshot


def snapshot(version=1):
    return HistorySnapshot(
        version,
        None,
        [
            {"timestamp": 2, "temperature": 27, "out_of_range": True},
            {"timestamp": 2, "temperature": 28, "out_of_range": True},
            {"timestamp": 3, "temperature": 25.5, "out_of_range": False},
            {"timestamp": 4, "temperature": 24, "out_of_range": True},
        ],
    )


def test_snapshot_preserves_equal_timestamp_order_and_inclusive_bounds():
    history = snapshot()
    assert json.loads(history.render(2, 2)) == [
        {"timestamp": 2, "temperature": 27},
        {"timestamp": 2, "temperature": 28},
    ]
    assert json.loads(history.render(2.1, 2.9)) == []
    assert json.loads(history.render(0, 1)) == []
    assert len(json.loads(history.render(outside=True))) == 3
    assert json.loads(history.render(intervals=True)) == [
        {"start_time": 2, "end_time": 3},
        {"start_time": 4, "end_time": None},
    ]
    assert (
        json.loads(HistorySnapshot(2, 5, history.points).render(intervals=True))[-1]["end_time"]
        == 5
    )


def test_cache_versions_and_point_budget_never_return_old_data_for_a_new_version():
    cache = HistoryCache(max_points=8, max_entries=3)
    cache.put("a", snapshot(2))
    cache.put("a", snapshot(1))
    assert cache.get("a", 2).version == 2
    assert cache.get("a", 3) is None
    cache.put("b", snapshot())
    cache.get("a", 2)
    cache.put("c", snapshot())
    assert cache.get("b", 1) is None
    assert cache.points == 8
    cache.put("a", None)
    assert cache.is_large("a") and cache.points == 4


def test_empty_histories_and_large_markers_also_have_an_entry_bound():
    cache = HistoryCache(max_points=8, max_entries=2)
    for i in range(100):
        cache.put(str(i), None if i % 2 else HistorySnapshot(0, None, []))
    assert len(cache.entries) == 2
    assert cache.points == 0
````

## tests/test\_load\_validation.py

Reject misleading aggregate/phase results, omitted offered work and exactly-ten/invalid latency observations.

SHA256: `62932f70a9c56654e1786258933a1be55c53e3b640e2f89403566ea93c93bf5c`

````python
import argparse
import asyncio

import pytest

from loadtests.http_profile import Phase, PhaseResult, parse_profile, run_profile
from loadtests.validation import meets_notification_deadline, variable_http_phases


@pytest.mark.parametrize(
    "latency,expected",
    [
        (None, False),
        (float("nan"), False),
        (float("inf"), False),
        (-1, False),
        (0, True),
        (9.999, True),
        (10, False),
        (10.001, False),
    ],
)
def test_deadline_is_strict_and_requires_valid_evidence(latency, expected):
    assert meets_notification_deadline(latency) is expected


@pytest.mark.parametrize("value", ["", "100", "-1:20", "100:0", "100:nan", "100:inf", "0:10"])
def test_invalid_profiles_are_rejected(value):
    with pytest.raises(argparse.ArgumentTypeError):
        parse_profile(value)


def test_a_later_easy_phase_does_not_hide_an_overloaded_phase():
    busy, recovery = parse_profile("200:20,50:20")
    slow = PhaseResult(busy, start=0, scheduled=4000, succeeded=4000, last_completed=25)
    fast = PhaseResult(recovery, start=20, scheduled=1000, succeeded=1000, last_completed=40)
    # The overall 5000/40 == 125 rps looks perfect. The busy phase actually only
    # achieved 160/200 rps once its five seconds of backlog are included.
    assert slow.report()["achieved_rps"] == 160
    assert not slow.report()["passed"]
    assert fast.report()["passed"]


def test_unsent_load_and_http_failures_cannot_pass():
    result = PhaseResult(Phase(100, 1), start=0, scheduled=95, succeeded=95)
    assert not result.report()["passed"]  # 95% throughput but load was dropped.
    result.scheduled = 100
    result.errors = 5
    assert not result.report()["passed"]


async def test_profile_accounts_for_exceptions_and_each_phase():
    async def query(index):
        if index == 1:
            raise RuntimeError("broken request")
        return index != 2

    results = await run_profile(
        [Phase(100, 0.02), Phase(100, 0.02)], query, asyncio.get_running_loop().time()
    )
    assert [r.scheduled for r in results] == [2, 2]
    assert [r.errors for r in results] == [1, 1]
    assert [r.succeeded for r in results] == [1, 1]
    assert results[0].unexpected_errors == ["RuntimeError"]
    assert all(not r.report()["passed"] for r in results)


async def test_overloaded_generator_bounds_tasks_and_fails_instead_of_dropping_silently():
    async def query(_):
        await asyncio.sleep(0.03)
        return True

    results = await run_profile(
        [Phase(1000, 0.01)], query, asyncio.get_running_loop().time(), max_pending=1
    )
    assert results[0].scheduled == 1
    assert results[0].report()["expected_requests"] == 10
    assert not results[0].report()["passed"]


def native_samples(slow=False):
    samples, completed = [], 0
    for second in range(40):
        target = 100 if second < 20 else 200
        effective = 100 if slow else target
        completed += effective
        samples.append(
            {
                "elapsed": second,
                "target_rps": target,
                "effective_rps": effective,
                "http_ok": completed,
            }
        )
    return samples


def test_native_variable_validation_requires_multiple_rates_and_keeps_up_at_each():
    assert variable_http_phases(native_samples())["passed"]
    assert not variable_http_phases(native_samples(slow=True))["passed"]
    assert not variable_http_phases(native_samples()[:20])["passed"]
    assert not variable_http_phases([])["passed"]


def test_native_rate_gauge_alone_cannot_hide_missing_completions():
    samples = native_samples()
    for sample in samples:
        sample["http_ok"] //= 2
    assert not variable_http_phases(samples)["passed"]
````

## tests/test\_notification.py

Delivery outcomes, retries/lease acknowledgement and strict deadline miss accounting.

SHA256: `a1509715e2aefd233df20077d6e4e08369cb269ab34ba6d314e2217b2d9c5585`

````python
import json
import time

import httpx
import pytest

from temperature_observability.common import Metrics
from temperature_observability.config import Settings
from temperature_observability.notification import NotificationHandler, retry_delay


@pytest.mark.parametrize(
    "status,outcome",
    [
        (200, "sent"),
        (400, "failed"),
        (422, "failed"),
        (429, "retry"),
        (503, "retry"),
        (401, "retry"),
    ],
)
async def test_delivery_classifies_and_acknowledges(status, outcome):
    results = []
    payload = {
        "experiment_id": "e",
        "measurement_id": "m",
        "notification_type": "OutOfRange",
        "researcher": "r@example.invalid",
        "cipher_data": "opaque",
    }

    def transport(request):
        if request.url.path == "/notify":
            assert json.loads(request.content) == payload
            assert request.headers["Idempotency-Key"] == "e:m:OutOfRange"
            return httpx.Response(status, json=0.1)
        results.append(json.loads(request.content))
        return httpx.Response(200, json={})

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        handler = NotificationHandler(
            Settings(notification_url="http://sink/notify"), client, Metrics()
        )
        await handler.deliver(
            {
                "id": 1,
                "payload": payload,
                "measured_at": time.time(),
                "lease_token": "lease",
                "attempts": 1,
            }
        )
    assert results[0]["outcome"] == outcome
    assert results[0]["lease_token"] == "lease"


def test_retry_delay_is_bounded():
    assert retry_delay(1) == 0.25
    assert retry_delay(100000) == 5
    assert retry_delay(1, "3") == 3
    assert retry_delay(1, "100000") == 60
    assert retry_delay(1, "invalid") == 0.25


@pytest.mark.parametrize("latency,misses", [(9.999, 0), (10, 1), (10.001, 1)])
async def test_deadline_metric_counts_every_late_delivery(monkeypatch, latency, misses):
    monkeypatch.setattr("temperature_observability.notification.time.time", lambda: 100)
    metrics = Metrics()
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200))
    ) as client:
        handler = NotificationHandler(
            Settings(notification_url="http://sink/notify"), client, metrics
        )
        await handler.deliver(
            {
                "id": 1,
                "payload": {
                    "experiment_id": "e",
                    "measurement_id": "m",
                    "notification_type": "OutOfRange",
                },
                "measured_at": 100 - latency,
                "lease_token": "t",
                "attempts": 1,
            }
        )
    assert metrics.registry.get_sample_value("to_notification_deadline_misses_total") == misses
````

## tests/test\_public\_api\_integration.py

Required public routes and query contract, including internal-route protection.

SHA256: `b62d7831506d8f3ba038334bbd55a2519777227e634fba2070e774aa917bf9ea`

````python
import os
from uuid import uuid4

import httpx
import pytest

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(
        os.environ.get("TO_TEST_STACK") != "1",
        reason="set TO_TEST_STACK=1 with local Compose running",
    ),
]


async def test_public_gateway_configuration_validation_and_isolation():
    experiment = "api-test-" + uuid4().hex
    body = {
        "experiment": experiment,
        "researcher": "researcher@example.invalid",
        "sensors": ["a", "b"],
        "temperature_range": {"lower_threshold": 25, "upper_threshold": 26},
    }
    async with httpx.AsyncClient(
        base_url="http://localhost:3003", trust_env=False, timeout=10
    ) as client:
        response = await client.post("/experiments", json=body)
        assert response.status_code == 200
        state = (await client.get("/experiments/" + experiment)).json()
        assert state["status"] == "configured" and state["sensors"] == ["a", "b"]
        assert (await client.post("/experiments", json=body)).status_code == 200
        conflicting = {**body, "researcher": "someone-else@example.invalid"}
        assert (await client.post("/experiments", json=conflicting)).status_code == 409
        assert (
            await client.get("/temperature/out-of-range", params={"experiment-id": experiment})
        ).json() == []
        for params in [
            {"experiment-id": experiment, "start-time": "nan", "end-time": 1},
            {"experiment-id": experiment, "start-time": 2, "end-time": 1},
            {"experiment-id": experiment},
        ]:
            assert (await client.get("/temperature", params=params)).status_code == 422
        assert (
            await client.get("/temperature/out-of-range", params={"experiment-id": "unknown"})
        ).status_code == 404
        assert (await client.post("/internal/tasks/commit", json={})).status_code == 404
        assert (await client.post("/internal/notifications/claim", json={})).status_code == 404
        document = await client.get("/openapi.json")
        assert document.status_code == 200 and "/temperature" in document.json()["paths"]
````

## tests/test\_scheduler.py

Ordering, reservation/capacity, backpressure and reassignment behavior; scaling policy boundaries.

SHA256: `16b311439108f98a54808dbaa7c5f20eccdd5964356540395ef575bc018b1007`

````python
from collections import deque

import httpx

from temperature_observability.autoscale import ScalingPolicy
from temperature_observability.common import Metrics
from temperature_observability.config import Settings
from temperature_observability.discovery import ExecutorPool, Worker
from temperature_observability.models import EventEnvelope
from temperature_observability.scheduler import KafkaConsumerAndTaskScheduler


def test_scheduler_reserves_least_busy_and_honors_capacity():
    pool = ExecutorPool("executor", 8000, None)
    pool.workers = {"a": Worker("a", 2, 3), "b": Worker("b", 0, 2)}
    assert pool.reserve().url == "b"
    assert pool.reserve().url == "b"
    assert pool.reserve().url == "a"
    assert pool.reserve().url == "a"  # Reported load 2; reservations haven't exceeded 2 yet.
    assert pool.reserve().url == "a"
    assert pool.reserve() is None


async def test_offsets_commit_only_after_successful_durable_task():
    calls = []

    def transport(request):
        calls.append("dispatch")
        return httpx.Response(503 if len(calls) == 1 else 200, json={"status": "committed"})

    class Consumer:
        def commit(self, offsets, asynchronous):
            assert not asynchronous
            calls.append(("commit", offsets[0].offset))

    async with httpx.AsyncClient(transport=httpx.MockTransport(transport)) as client:
        scheduler = KafkaConsumerAndTaskScheduler(Settings(), client, Metrics())
        scheduler.consumer = Consumer()
        scheduler.queues[0] = deque([EventEnvelope(offset=8, kind="experiment_started")])
        scheduler.executors.workers = {"a": Worker("http://a", 0, 1)}
        await scheduler.dispatch_partition(0)
        scheduler.thread.shutdown()
    assert calls == ["dispatch", "dispatch", ("commit", 9)]


def test_scaling_uses_sustained_load_and_downscale_cooldown():
    policy = ScalingPolicy(minimum=1, maximum=3)
    busy = {"executors": 1, "queued_events": 2000, "active_tasks": 8, "capacity": 8}
    assert policy.decide(busy, 0) == 1
    assert policy.decide(busy, 4) == 1
    assert policy.decide(busy, 5) == 2
    idle = {"executors": 2, "queued_events": 0, "active_tasks": 0, "capacity": 16}
    assert policy.decide(idle, 6) == 2
    assert policy.decide(idle, 65) == 2
    assert policy.decide(idle, 66) == 1


async def test_hot_partition_is_paused_without_stalling_other_partitions():
    calls = []

    class Consumer:
        def pause(self, partitions):
            calls.append(("pause", [p.partition for p in partitions]))

        def resume(self, partitions):
            calls.append(("resume", [p.partition for p in partitions]))

    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
    scheduler.consumer = Consumer()
    scheduler.on_assign([0, 1, 2, 3])
    try:
        scheduler.queues[0].extend([None] * 300)
        await scheduler.apply_backpressure()
        assert calls == [("pause", [0])]
        assert not scheduler.paused
        scheduler.queues[0].clear()
        scheduler.inflight[1] = 256  # Count tasks already assigned, not just queued work.
        await scheduler.apply_backpressure()
        assert calls[-2:] == [("pause", [1]), ("resume", [0])]
        scheduler.on_revoke([1])
        assert 1 not in scheduler.paused_partitions
    finally:
        scheduler.thread.shutdown()


async def test_global_backpressure_retains_hysteresis_and_assignment_resets_pauses():
    class Consumer:
        def pause(self, partitions):
            pass

        def resume(self, partitions):
            pass

    scheduler = KafkaConsumerAndTaskScheduler(Settings(queue_capacity=1024), None, Metrics())
    scheduler.consumer = Consumer()
    scheduler.on_assign([0, 1, 2, 3])
    try:
        for queue in scheduler.queues.values():
            queue.extend([None] * 256)
        await scheduler.apply_backpressure()
        assert scheduler.paused and scheduler.paused_partitions == {0, 1, 2, 3}
        for queue in scheduler.queues.values():
            while len(queue) > 200:
                queue.pop()
        await scheduler.apply_backpressure()
        assert scheduler.paused  # Still above the global low watermark.
        for queue in scheduler.queues.values():
            queue.clear()
        await scheduler.apply_backpressure()
        assert not scheduler.paused_partitions and not scheduler.paused
        scheduler.paused_partitions.add(0)
        scheduler.on_assign([0, 1, 2, 3])
        assert not scheduler.paused_partitions
    finally:
        scheduler.thread.shutdown()
````

## tests/test\_service\_client.py

Shared HTTP transport response/error compatibility and compressed responses.

SHA256: `390cf502e90432f9abed69950e728799367282cb6fab0a42451edd4ed940421f`

````python
import asyncio
import gzip

import httpx
import pytest

from temperature_observability.common import http_client


async def test_service_client_decodes_gzip_once():
    body = gzip.compress(b'{"status":"ok"}')

    async def handle(reader, writer):
        await reader.readuntil(b"\r\n\r\n")
        writer.write(
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n"
            b"Content-Encoding: gzip\r\nContent-Length: "
            + str(len(body)).encode()
            + b"\r\nConnection: close\r\n\r\n"
            + body
        )
        await writer.drain()
        writer.close()
        await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    async with server, http_client() as client:
        port = server.sockets[0].getsockname()[1]
        response = await client.get(f"http://127.0.0.1:{port}")
        assert response.json() == {"status": "ok"}


async def test_service_client_normalizes_connection_errors():
    async with http_client() as client:
        with pytest.raises(httpx.TransportError):
            await client.get("http://127.0.0.1:1", timeout=0.1)
````

