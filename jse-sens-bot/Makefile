.PHONY: install prereqs test lint db-init dry-run fetch-live fetch-live-all parse review etl-offline docker-up docker-down docker-browser-install docker-live-fetch docker-live-fetch-all

install:
	python3 -m pip install -r requirements-dev.txt
	python3 -m playwright install chromium

prereqs:
	./check_prereqs.sh

test:
	python3 -m unittest discover -s tests -v

db-init:
	python3 -m mvp_sens.scripts.db_insert

dry-run:
	python3 -m mvp_sens.scripts.fetch_sens --dry-run --limit 5

fetch-live:
	python3 -m mvp_sens.scripts.fetch_sens --limit 20

fetch-live-all:
	python3 -m mvp_sens.scripts.fetch_sens --limit 20 --include-all --skip-download

parse:
	python3 -m mvp_sens.scripts.parse_pdf --limit 10

review:
	python3 -m mvp_sens.scripts.gpt_review --limit 10

etl-offline:
	python3 -m mvp_sens.scripts.run_etl --skip-fetch --parse-limit 10 --review-limit 10

docker-up:
	docker compose up --build

docker-down:
	docker compose down

docker-browser-install:
	docker compose run --rm sens-scraper python -m playwright install chromium

docker-live-fetch:
	docker compose run --rm sens-scraper python -m playwright install chromium
	docker compose run --rm sens-scraper python -m mvp_sens.scripts.fetch_sens --limit 20

docker-live-fetch-all:
	docker compose run --rm sens-scraper python -m playwright install chromium
	docker compose run --rm sens-scraper python -m mvp_sens.scripts.fetch_sens --limit 20 --include-all --skip-download
