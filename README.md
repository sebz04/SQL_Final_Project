# SQL_Final_Project

**Nonprofit Accountability in Los Angeles: A Data-Driven Analysis**

***Overview***

This project investigates financial accountability and compensation practices among nonprofit organizations in Los Angeles County. Using a blend of ProPublica’s Nonprofit Explorer API, web scraping, and government open data, the analysis reveals critical trends in executive compensation, organizational financial health, and the broader impact of privatized public services.

This project was created as part of my SQL Final Portfolio to demonstrate skills in data extraction, ETL pipelines, SQL analytics, and data storytelling.

***Key Questions Explored***

Which nonprofits allocate unusually high percentages of their expenses to executive compensation?

Are nonprofits with high compensation ratios running deficits or delaying other responsibilities?

How do nonprofit financial trends compare across ZIP codes in Los Angeles?

***Tech Stack***

Languages: Python, SQL

Libraries: requests, beautifulsoup4, pandas, sqlalchemy, psycopg2, ThreadPoolExecutor

Database: PostgreSQL

Cloud: AWS RDS (PostgreSQL)

Pipeline & Workflow: GitHub Actions

Visualization: Looker Studio

Version Control: Git + GitHub

***Data Sources***

ProPublica Nonprofit Explorer API

ProPublica Nonprofit Explorer Web Pages

***Insights***

Some nonprofits report executive compensation exceeding 100% of total expenses, suggesting possible mismanagement or requrining further inquiry.

ZIP code analysis reveals geographic disparities in revenue, spending, and asset accumulation patterns across Los Angeles.

***Features***

-- Automated ETL pipeline from API/web to database and raw schema to cleaned staging schema

-- Staging + warehouse schema for clean data modeling 

-- SQL queries and dashboards for exploration & insight

-- Scheduled refreshes using GitHub Actions

***Author***

Sebastian Zapata-Ochoa

Undergraduate Student – Information Systems and Business Analytics & Journalism

