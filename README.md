# 📚 Amazon Books DE Analytics ML Pipeline
By "Dui" Watcharapong Moonrin

An end-to-end **data engineering portfolio project** that prepares Amazon Books raw data for two downstream use cases:

- **Analytics / reporting** for Data Analysts
- **Curated review data** for future **ML / NLP** work by Data Scientists

This project focuses on building a practical DE pipeline using **PySpark, Google Cloud Storage, Dataproc, BigQuery, and Looker Studio** to transform raw book and review data into clean, reusable serving layers.

---

## 🚀 Project Overview

Raw Amazon Books data is often not immediately ready for analytics or machine learning workflows.  
This project simulates the role of a **Data Engineer** who prepares data for downstream consumers by building a lightweight end-to-end pipeline.

The pipeline is designed to:

- ingest raw Amazon Books data
- clean and standardize book and review records
- publish analytics-ready serving views
- publish DS-ready curated review data
- demonstrate downstream consumption through dashboard and report examples

---

## 🎯 Business Use Case

![Business Use Case](docs/images/business_use_case.png)

**Amazon Books Data ~(2GB)**
- **Data Analyst (DA)** needs clean, analytics-ready outputs for dashboards and reports because the raw dataset is too large and too messy for direct spreadsheet or BI use.
- **Data Scientist (DS)** needs curated review-level text data for future NLP and sentiment analysis use cases.
- **Data Engineer (DE)** responds by defining output requirements and refresh frequency first, then designing a **batch-oriented pipeline** to serve both downstream consumers.

---

## 🏗 Architecture Diagram

![Architecture Diagram](docs/images/architecture_diagram.png)

### Architecture Layers / Tech Stack

- **Source Layer**: Amazon Books raw dataset (`book_details`, `reviews`) as the upstream data source  
- **Storage Layer (Bronze)**: **Google Cloud Storage (GCS)** for raw and landing data zones  
- **Processing Layer (Silver)**: **PySpark** jobs executed on **Dataproc** for data cleansing, standardization, and transformation  
- **Orchestration Layer**: **Apache Airflow (Cloud Composer)** for batch workflow orchestration and task scheduling  
- **Serving Layer (Gold)**: **BigQuery** serving **analytics-ready views** and **DS-ready curated datasets**  
- **Consumption Layer**: **Looker Studio** dashboards and reports for analytics consumption  
- **Development Layer**: **Docker** for local reproducible environments and **GitHub** for version-controlled delivery

## 🗂 Dataset

This project uses the [**Amazon Books Reviews dataset**](https://www.kaggle.com/datasets/mohamedbakhet/amazon-books-reviews/data), consisting of approximately **3 GB of raw data** across two primary tables.

_The files have information about 3M book reviews for 212,404 unique book and users who gives these reviews for each book, monthly update._

### 📊 Data Overview

| Table Name     | Description                     | Rows       | Size    |
|----------------|----------------------------------|------------|---------|
| `books_data` | Book-level metadata              | 212,404    | ~181 MB |
| `books_rating` | Review-level transactional data  | 3,000,000  | ~2.86 GB |

---

### 🧱 Entity Relationship (ERD)

```mermaid
erDiagram
    Direction LR
    BOOKS_DATA ||--o{ BOOKS_RATING : has

    BOOKS_DATA {
        int book_id PK
        string title
        string description
        string authors
        string publisher
        date publishedDate
        string categories
        int ratingsCount
    }

    BOOKS_RATING {
        int id PK
        int book_id FK
        float price
        string user_id
        string profileName
        int review_score
        datetime review_time
        string review_summary
        string review_text
    }