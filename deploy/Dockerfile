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
