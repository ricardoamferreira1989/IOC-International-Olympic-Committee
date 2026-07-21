# IOC Analytics Platform

## Overview

This project was developed as part of a Data Engineering case study for the International Olympic Committee (IOC).

The solution implements a batch analytics platform using Python and Apache Spark following a Medallion Architecture (Bronze, Silver, and Gold). The platform supports analytics workloads and demonstrates dimensional modelling, Slowly Changing Dimensions (SCD), and data governance concepts.

---

## Architecture

The platform follows a Medallion Architecture:

- **Bronze** – Raw data ingestion from the source CSV.
- **Silver** – Data cleansing and transformations.
- **Gold** – Star schema creation for analytical reporting.

Governance is represented by **Apache Atlas**, and orchestration is demonstrated using **Apache Airflow**.

---

## Project Structure

IOC-Analytics-Platform/
│
├── data/
│   ├── source/
│   ├── bronze/
│   ├── silver/
│   └── gold/
│
├── src/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── pipeline.py
│   └── main.py
│
├── orchestration/
│   └── olympic_pipeline_dag.py
│
├── README.md
├── requirements.txt
└── mypy.ini
```

---

## Pipeline

The pipeline consists of three stages:

1. Bronze – Ingest raw CSV data into Parquet.
2. Silver – Clean and transform the data.
3. Gold – Build dimension and fact tables using a star schema.

The `dim_athlete` dimension implements **Slowly Changing Dimension Type 2**, preserving historical changes to athlete height and weight.

---

## Technologies

- Python
- Apache Spark (PySpark)
- Apache Airflow
- Apache Atlas
- Parquet

---

## Repository

This repository contains the architecture design, dimensional model, and Python implementation required for the case study.