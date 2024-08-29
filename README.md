# ETL pipeline with recommendation system for TMDB movies using Airflow, Spark, Python, Docker, BigQuery, Google Cloud Storage

This project presents an end-to-end data pipeline solution for extracting, transforming and loading (ETL) trending movie data from the TMDB API into the BigQuery data warehouse. 
Subsequently, a container-based recommendation system is created that proposes the 3 most similar videos to the one watched.
The pipeline leverages a combination of tools and services including Apache Airflow, Apache Spark, Python, Docker, BigQuery, Google Cloud Storage.

## Table of Contents

- [Overview](#overview)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [System Setup](#system-setup)

## Overview

Pipelines is designed to:

1. Extract data from TMDB using its API.
2. Store the raw data into an Google Cloud Storage bucket using Airflow.
3. Transform the data with Spark and Python.
4. Load the transformed data into Google BigQuery using Airflow.
5. Create content based recommendation system for trending movies using Spark and Python
6. Load recommendations into Google BigQuery.

## Architecture

1. **TMDB API**: Source of the data.
2. **Apache Airflow**: Orchestrates and manages tasks.
3. **Apache Spark**:  Analytics engine for data processing. Using with Python to transform and create recommendation system.
4. **Docker**: Platform for running our project in an isolated environment using containers
5. **Google Cloud Storage**: Storage for our data.
6. **Google BigQuery**: Data warehousing.

## Prerequisites
- Google Cloud service account with appropriate permissions for Cloud Storage (Storage Admin, Storage object Admin), BigQuery (BigQuery Admin).
- TMDB API credentials.
- Docker Installation (version 4.33.1.)
- Python 3.9 or higher

## System Setup
1. Clone the repository.
   ```bash
    git clone https://github.com/uminskib/Trending_movies_ETL_Recommendation.git
   ```
2. Add your credentials in gcp_credentials.json and tmdb_api_key.json files (config folder).
   
4. Starting the containers
   ```bash
    docker-compose up --build
   ```
5. Launch the Airflow web UI.
   ```bash
    open http://localhost:8080
   ```
