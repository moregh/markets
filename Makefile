.PHONY: start stop reset logs ps build

DC := docker compose -f docker/docker-compose.yml --env-file .env

start:
	./start.sh

stop:
	./stop.sh

reset:
	./reset.sh

build:
	$(DC) build api

logs:
	$(DC) logs -f

ps:
	$(DC) ps
