FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PYTHONPATH=/opt/uriesmooth-stack \
    HOME=/home/alphauriel \
    ALPHAURIEL_HOME=/var/lib/uriesmooth-stack \
    ALPHAURIEL_LOG_FORMAT=json \
    ALPHAURIEL_LOG_LEVEL=INFO

RUN groupadd --gid 10001 alphauriel \
    && useradd \
        --uid 10001 \
        --gid 10001 \
        --create-home \
        --home-dir /home/alphauriel \
        --shell /usr/sbin/nologin \
        alphauriel \
    && mkdir -p \
        /opt/uriesmooth-stack \
        /var/lib/uriesmooth-stack \
    && chown -R 10001:10001 \
        /opt/uriesmooth-stack \
        /var/lib/uriesmooth-stack

WORKDIR /opt/uriesmooth-stack

COPY --chown=10001:10001 pyproject.toml ./
COPY --chown=10001:10001 alphauriel_pascal ./alphauriel_pascal
COPY --chown=10001:10001 apps ./apps
COPY --chown=10001:10001 scripts/container-healthcheck.py \
    ./scripts/container-healthcheck.py

USER 10001:10001

ENTRYPOINT ["python", "-m", "alphauriel_pascal.cli"]

CMD ["worker", "--workers", "1"]
