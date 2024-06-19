batch_up:
	docker compose -f docker-compose.yaml up -d
batch_down:
	docker compose -f docker-compose.yaml down

stream_up:
	docker compose -f stream_processing/docker-compose.yaml up -d
stream_down:
	docker compose -f stream_processing/docker-compose.yaml down

run_all:
	docker compose -f docker-compose.yaml up -d
	docker compose -f stream_processing/docker-compose.yaml up -d

stop_all:
	docker compose -f docker-compose.yaml down
	docker compose -f stream_processing/docker-compose.yaml down