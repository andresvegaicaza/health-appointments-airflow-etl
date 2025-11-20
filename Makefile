# --------------------------------------------------
# Airflow + Postgres local environment controls
# --------------------------------------------------

# Start all services
up:
	docker compose up -d

# Stop services (but keep volumes)
down:
	docker compose down

# Stop services and remove volumes
clean:
	docker compose down -v

# Restart Airflow (webserver, scheduler, worker)
restart:
	docker compose restart airflow-webserver airflow-scheduler airflow-worker

# View logs for Airflow scheduler
logs-scheduler:
	docker compose logs -f airflow-scheduler

# View logs for entire stack
logs:
	docker compose logs -f

# Rebuild containers (if Dockerfile changes)
build:
	docker compose build

# Show running containers
ps:
	docker compose ps
