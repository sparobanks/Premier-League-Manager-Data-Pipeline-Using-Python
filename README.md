# Premier League Manager Data Pipeline Using Python

Automated pipeline for collecting and structuring Premier League manager data from Transfermarkt using Python.

This project demonstrates how web scraping can be used to transform unstructured web data into structured datasets suitable for analytics and research.

The system automatically collects manager profile information and coaching career history from Transfermarkt and converts the data into clean tabular datasets.

# Project Overview

Sports analytics platforms rely on structured datasets, but many valuable sources of football data exist only as web pages. Transfermarkt contains detailed information about managers, but the data is distributed across multiple pages and not directly available for programmatic analysis.

This project builds a Python-based data pipeline that:

* identifies current Premier League managers
* visits each manager’s profile page
* extracts manager profile information
* extracts full coaching career history
* cleans and structures the data
* outputs analysis-ready datasets

The project simulates a real-world data engineering workflow where external web data must be collected and prepared before analysis.


# Data Source

The data is collected from **Transfermarkt**, one of the most widely used football statistics platforms.

Example manager profile page:

[https://www.transfermarkt.com/mikel-arteta/profil/trainer/47620](https://www.transfermarkt.com/mikel-arteta/profil/trainer/47620)

Manager discovery page:

[https://www.transfermarkt.com/premier-league/trainer/pokalwettbewerb/GB1](https://www.transfermarkt.com/premier-league/trainer/pokalwettbewerb/GB1)


# Data Pipeline Architecture

The project follows a simple data pipeline structure:

```
Transfermarkt Website
        ↓
Python Scraper
        ↓
HTML Parsing
        ↓
Data Extraction
        ↓
Data Cleaning
        ↓
Structured Dataset (CSV)
        ↓
Ready for Analysis
```

This workflow demonstrates how raw web content can be transformed into structured datasets.


# Data Collected

## Manager Profile Data

The scraper extracts the following profile information:

* Manager name
* Full name
* Date of birth
* Age
* Place of birth
* Citizenship
* Contract duration
* Coaching licence
* Preferred formation
* Agent
* Transfermarkt manager ID
* Profile URL


## Coaching Career Data

The project also extracts full coaching history including:

* Club managed
* Managerial role
* Start date
* End date
* Duration
* Matches managed
* Points per match

# Tools and Technologies

The project was built using the following tools.

### Python

Used as the core programming language for building the scraping pipeline.

### Requests

Used to send HTTP requests and retrieve HTML pages.

### BeautifulSoup

Used to parse HTML content and extract relevant information from Transfermarkt pages.

### Pandas

Used to structure and clean the scraped data into tabular format.

# Installation

Clone the repository:

```bash
git clone https://github.com/sparobanks/Premier-League-Manager-Data-Pipeline-Using-Python.git
cd transfermarkt-manager-data-pipeline
```

Install required dependencies:

```bash
pip install -r requirements.txt
```


# Running the Scraper

Run the main scraper script:

```bash
python scraper.py
```

The scraper will:

* identify Premier League managers
* visit each manager profile
* extract manager information
* extract coaching history
* generate structured datasets


# Output

After running the scraper, the following datasets will be generated:

```
data/
   managers.csv
   career_history.csv
```

These files contain structured information about manager profiles and coaching history.


# Example Output

| Manager       | Club            | Start | End     | Matches | Points per Match |
| ------------- | --------------- | ----- | ------- | ------- | ---------------- |
| Pep Guardiola | Manchester City | 2016  | Present | 400+    | 2.35             |
| Mikel Arteta  | Arsenal         | 2019  | Present | 180     | 1.95             |

These datasets can be used for further analysis such as:

* managerial performance comparisons
* tactical trend analysis
* coaching career progression studies


# Project Structure

```
Premier-League-Manager-Data-Pipeline-Using-Python
│
├── scraper.py
├── requirements.txt
├── data
│   ├── managers.csv
│   └── career_history.csv
│
│
└── README.md
```


# Skills Demonstrated

This project demonstrates several important data science and data engineering skills:

* Web scraping
* Data extraction from HTML pages
* Data cleaning and structuring
* Automated data pipelines
* Working with real-world datasets

# Potential Extensions

Future improvements could include:

* storing the dataset in a SQL database
* building a football analytics dashboard
* scraping player statistics
* building an API for football data
* performing machine learning analysis on managerial performance

---

# Author

**Jasper Chinedu Nwangere**

Data Scientist | Data Analyst | Machine Learning

Portfolio
datawithjasper.com

GitHub
github.com/sparobanks
