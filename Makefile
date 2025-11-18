COMPOSE := docker-compose -f docker/docker-compose.yml --env-file .env

.PHONY: build-services start-services stop-services restart-services ps

build-services:
	@echo "Building Docker services..."
	$(COMPOSE) build

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