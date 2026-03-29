FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1
ENV PLAYWRIGHT_SKIP_BROWSER_DOWNLOAD=1

COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt
RUN python -m playwright install-deps chromium

COPY mvp_sens /app/mvp_sens

CMD ["python", "-m", "mvp_sens.scripts.run_etl", "--skip-fetch", "--parse-limit", "10"]
