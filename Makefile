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
