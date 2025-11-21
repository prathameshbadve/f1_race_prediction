COMPOSE := docker-compose --env-file .env

.PHONY: build-services start-services stop-services restart-services ps

build-services:
	@echo "Building Docker services..."
	$(COMPOSE) build

build-services-no-cache:
	@echo "Building Docker services without cache..."
	$(COMPOSE) build --no-cache

start-services:
	@echo "Starting all services..."
	$(COMPOSE) up -d
	@echo ""
	@echo "Services started! Access them at:"
	@echo "  MinIO Console:  http://localhost:9001"
	@echo "  MLflow UI:      http://localhost:5000"
	@echo "  Dagster UI:     http://localhost:3000"
	@echo "  PostgreSQL:     localhost:5432"

# Stop all services
stop-services:
	@echo "Stopping all services..."
	$(COMPOSE) down

# Restart all services
restart-services:
	@echo "Restarting all services..."
	$(COMPOSE) restart

# List running containers
ps:
	$(COMPOSE) ps

# Helper to validate .env is loaded correctly
validate-env:
	@echo "Validating environment variables..."
	@$(COMPOSE) config > /dev/null && echo "✅ Environment variables loaded successfully"

# Logs for individual services
logs-%:
	$(COMPOSE) logs -f $*

# Postgres CLI
postgres-cli:
	docker exec -it f1-postgres psql -U postgres

# Start individual services
start-%:
	$(COMPOSE) up -d $*

# Delete log files
clean-logs:
	@echo "Deleting log files..."
	rm -f monitoring/logs/*.log
	@echo "Log files deleted."
	@echo "----------------------"
	@echo "Creating new empty log fiels..."
	touch monitoring/logs/app.log monitoring/logs/error.log monitoring/logs/data_ingestion.log monitoring/logs/data_processing.log monitoring/logs/resources.log monitoring/logs/fastf1.log
	@echo "New empty log files created."