Financial Markets Intelligence

A Python-based project for collecting, processing, and analysing financial market data, with an initial focus on the Johannesburg Stock Exchange (JSE). Designed to support analysts, data scientists, and automated pipelines for financial insights.

Table of Contents

Project Overview
Project Structure
Environment Setup
Version Control & Workflow
Data Sources
Running Scripts & Notebooks
Contributing
License



Project Overview

This project provides:

Data collection: Fetch historical and real-time JSE market data.
Data processing: Clean, normalize, and enrich datasets.
Analysis & Insights: Use notebooks and scripts to generate actionable insights.
Reproducibility: Environment management and version control for consistent workflows.

Project Structure
financial-markets-intelligence/
├── data/                 # Raw and processed datasets
│   ├── raw/              # Original source data
│   └── processed/        # Cleaned/enriched datasets
├── notebooks/            # Jupyter notebooks for analysis & visualization
├── scripts/              # Python scripts: ETL, API fetch, preprocessing
├── tests/                # Unit and integration tests
├── config/               # Config files, API keys, secrets
├── logs/                 # Script execution logs
├── venv/                 # Python virtual environment (ignored in Git)
├── .gitignore
├── requirements.txt      # Python dependencies
└── README.md
Environment Setup
Clone repository & create virtual environment
git clone git@github.com:Joshua-DuPlessis/financial-markets-intelligence.git
cd financial-markets-intelligence
python3 -m venv venv
source venv/bin/activate
Install dependencies
pip install --upgrade pip
pip install -r requirements.txt
Verify installation
python -m pip list

Note: Create a .env file in config/ to store API keys or credentials.

Version Control & Workflow
Commit frequently; keep one logical change per commit.
Use descriptive commit messages, e.g., feat: add JSE data fetch script.
For new features or experiments, create a branch:
git checkout -b feature/data-fetch
Merge to main only when stable and tested.
Pull often to stay up-to-date:
git pull origin main
Data Sources
Primary: JSE historical market data (e.g., stock prices, volumes)
Secondary: Financial news feeds, SEC filings, exchange reports
Format: CSV, JSON, or API endpoints
Storage: data/raw → data/processed

Scripts in scripts/ will automate fetching, cleaning, and storing data.

Running Scripts & Notebooks
Scripts: Run directly with Python:
python scripts/fetch_jse_data.py
Notebooks: Start Jupyter:
jupyter notebook
Always log outputs to logs/ for reproducibility.
Contributing
Fork the repository
Create a feature branch
Write clear, tested code
Submit pull requests with detailed descriptions

License

GNU License — see LICENSE file.