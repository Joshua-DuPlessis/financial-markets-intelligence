.PHONY: install prereqs test lint db-init dry-run fetch-live fetch-live-all parse review etl-offline scheduler-smoke audit-report analyst-since-last-run analyst-intraday analyst-daily-delta analyst-release-signals phase1-signoff docker-up docker-down docker-browser-install docker-live-fetch docker-live-fetch-all docker-scheduler-up docker-scheduler-down

install:
	python3 -m pip install -r requirements-dev.txt
	python3 -m playwright install chromium

prereqs:
	./check_prereqs.sh

test:
	python3 -m unittest discover -s tests -v

phase1-signoff:
	python3 -m unittest discover -s tests -v
	python3 -m compileall -q mvp_sens tests
	python3 -m mvp_sens.scripts.scheduler_loop --dry-run --skip-download --max-iterations 1 --limit 5
	python3 -m mvp_sens.scripts.audit_report --run-limit 10 --alert-limit 20

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

scheduler-smoke:
	python3 -m mvp_sens.scripts.scheduler_loop --dry-run --skip-download --max-iterations 1 --limit 5

audit-report:
	python3 -m mvp_sens.scripts.audit_report --run-limit 10 --alert-limit 20

analyst-since-last-run:
	python3 -m mvp_sens.scripts.analyst_outputs since-last-run --format json

analyst-intraday:
	python3 -m mvp_sens.scripts.analyst_outputs intraday-snapshot --format csv

analyst-daily-delta:
	python3 -m mvp_sens.scripts.analyst_outputs daily-delta --format json

analyst-release-signals:
	python3 -m mvp_sens.scripts.analyst_outputs release-signals --format json

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

docker-scheduler-up:
	docker compose --profile scheduler up -d sens-scheduler

docker-scheduler-down:
	docker compose --profile scheduler stop sens-scheduler
