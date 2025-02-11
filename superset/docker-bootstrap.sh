#!/usr/bin/env bash

set -eo pipefail

# Make python interactive
if [ "$DEV_MODE" == "true" ]; then
    if [ "$(whoami)" = "root" ] && command -v uv > /dev/null 2>&1; then
      echo "Reinstalling the app in editable mode"
      uv pip install -e .
    fi
fi
REQUIREMENTS_LOCAL="/app/docker/requirements-local.txt"
PORT=${PORT:-8088}
# If Cypress run – overwrite the password for admin and export env variables
if [ "$CYPRESS_CONFIG" == "true" ]; then
    export SUPERSET_TESTENV=true
    export POSTGRES_DB=superset_cypress
    export SUPERSET__SQLALCHEMY_DATABASE_URI=postgresql+psycopg2://superset:superset@db:5432/superset_cypress
    PORT=8081
fi
if [[ "$DATABASE_DIALECT" == postgres* ]] && [ "$(whoami)" = "root" ]; then
    # older images may not have the postgres dev requirements installed
    echo "Installing postgres requirements"
    if command -v uv > /dev/null 2>&1; then
        # Use uv in newer images
        uv pip install -e .[postgres]
    else
        # Use pip in older images
        pip install -e .[postgres]
    fi
fi
#
# Make sure we have dev requirements installed
#
if [ -f "${REQUIREMENTS_LOCAL}" ]; then
  echo "Installing local overrides at ${REQUIREMENTS_LOCAL}"
  uv pip install --no-cache-dir -r "${REQUIREMENTS_LOCAL}"
else
  echo "Skipping local overrides"
fi

case "${1}" in
  worker)
    echo "Starting Celery worker..."
    # setting up only 2 workers by default to contain memory usage in dev environments
    celery --app=superset.tasks.celery_app:app worker -O fair -l INFO --concurrency=${CELERYD_CONCURRENCY:-2}
    ;;
  beat)
    echo "Starting Celery beat..."
    rm -f /tmp/celerybeat.pid
    celery --app=superset.tasks.celery_app:app beat --pidfile /tmp/celerybeat.pid -l INFO -s "${SUPERSET_HOME}"/celerybeat-schedule
    ;;
  app)
    echo "Starting web app (using development server)..."
    flask run -p $PORT --with-threads --reload --debugger --host=0.0.0.0
    ;;
  app-gunicorn)
    echo "Starting web app..."
    /usr/bin/run-server.sh
    ;;
  *)
    echo "Unknown Operation!!!"
    ;;
esac