# STEDI Human Balance Analytics Data Lake

## Project Overview

This project implements a complete AWS Data Lake for the STEDI Human Balance Analytics application using the **Landing → Trusted → Curated** architecture.

The pipeline ingests customer, accelerometer, and step trainer data from Amazon S3, transforms the data using AWS Glue Visual ETL, stores processed datasets in Amazon S3, and registers metadata in the AWS Glue Data Catalog.

The final curated dataset is prepared for machine learning by combining accelerometer sensor readings with step trainer measurements.

---

# Architecture

```
Landing Zone (JSON)
        │
        ▼
Trusted Zone (Parquet)
        │
        ▼
Curated Zone (Parquet)
        │
        ▼
Machine Learning Dataset
```

---

# AWS Services Used

- Amazon S3
- AWS Glue Studio (Visual ETL)

- Amazon Athena
- AWS IAM

---

# Project Structure

```
stedi-ak-bucket/

customer/
│── landing/
│── trusted/
│── curated/

accelerometer/
│── landing/
│── trusted/

step_trainer/
│── landing/
│── trusted/

machine_learning/
└── curated/
```

---

# Landing Zone

The landing zone stores the raw JSON files uploaded to Amazon S3.

## Landing Tables

### customer_landing

| Column |
|---------|
| customerName |
| email |
| phone |
| birthDay |
| serialNumber |
| registrationDate |
| lastUpdateDate |
| shareWithResearchAsOfDate |
| shareWithPublicAsOfDate |
| shareWithFriendsAsOfDate |

---

### accelerometer_landing

| Column |
|---------|
| user |
| timestamp |
| x |
| y |
| z |

---

### step_trainer_landing

| Column |
|---------|
| sensorReadingTime |
| serialNumber |
| distanceFromObject |

---

# Trusted Zone

The trusted zone contains validated data that satisfies project privacy requirements.

## customer_trusted

Source:

customer_landing

Transformation:

```
shareWithResearchAsOfDate != 0
```

Purpose:

Only customers who agreed to share research data are retained.

Output:

```
customer/trusted/
```

---

## accelerometer_trusted

Sources

- customer_trusted
- accelerometer_landing

SQL

```sql
SELECT DISTINCT accelerometer_landing.*
FROM customer_trusted
INNER JOIN accelerometer_landing
ON customer_trusted.email = accelerometer_landing.user;
```

Purpose

Keep accelerometer records belonging only to trusted customers.

Output

```
accelerometer/trusted/
```

---

## step_trainer_trusted

Sources

- customer_curated
- step_trainer_landing

SQL

```sql
SELECT DISTINCT step_trainer_landing.*
FROM step_trainer_landing
INNER JOIN customer_curated
ON step_trainer_landing.serialNumber = customer_curated.serialNumber;
```

Purpose

Keep step trainer records belonging to curated customers.

Output

```
step_trainer/trusted/
```

---

# Curated Zone

The curated zone prepares data for analytics and machine learning.

---

## customer_curated

Sources

- customer_trusted
- accelerometer_trusted

SQL

```sql
SELECT DISTINCT customer_trusted.*
FROM accelerometer_trusted
INNER JOIN customer_trusted
ON accelerometer_trusted.user = customer_trusted.email;
```

Purpose

Retain customers that actually generated accelerometer data.

Output

```
customer/curated/
```

---

## machine_learning_curated

Sources

- accelerometer_trusted
- step_trainer_trusted

SQL

```sql
SELECT
    accelerometer_trusted.user,
    accelerometer_trusted.timestamp,
    accelerometer_trusted.x,
    accelerometer_trusted.y,
    accelerometer_trusted.z,
    step_trainer_trusted.distanceFromObject
FROM accelerometer_trusted
INNER JOIN step_trainer_trusted
ON accelerometer_trusted.timestamp =
step_trainer_trusted.sensorReadingTime;
```

Purpose

Create the final dataset for machine learning.

Output

```
machine_learning/curated/
```

---

# Final Dataset Schema

| Column | Type |
|---------|------|
| user | STRING |
| timestamp | BIGINT |
| x | DOUBLE |
| y | DOUBLE |
| z | DOUBLE |
| distanceFromObject | INT |

---

# Glue Jobs

The following AWS Glue Visual ETL jobs were implemented.

| Glue Job |
|-----------|
| customer_landing_to_trusted |
| accelerometer_landing_to_trusted |
| customer_trusted_to_curated |
| step_trainer_trusted |
| machine_learning_curated |

---

# Data Formats

| Zone | Format |
|------|--------|
| Landing | JSON |
| Trusted | Parquet (Snappy Compression) |
| Curated | Parquet (Snappy Compression) |

---

# Expected Row Counts

## Landing

| Table | Rows |
|---------|------|
| customer_landing | 956 |
| accelerometer_landing | 81,273 |
| step_trainer_landing | 28,680 |

---

## Trusted

| Table | Rows |
|---------|------|
| customer_trusted | 482 |
| accelerometer_trusted | 40,981 |
| step_trainer_trusted | 14,460 |

---

## Curated

| Table | Rows |
|---------|------|
| customer_curated | 482 |
| machine_learning_curated | 43,681 |

---

# Project Workflow

```
Raw JSON Files
        │
        ▼
Amazon S3 Landing Zone
        │
        ▼
AWS Glue Data Catalog
        │
        ▼
AWS Glue Visual ETL
        │
        ▼
Trusted Zone (Parquet)
        │
        ▼
Curated Zone (Parquet)
        │
        ▼
Machine Learning Curated Dataset
```

---

# Key Concepts

- Data Lake Architecture
- Landing → Trusted → Curated Design
- AWS Glue Visual ETL
- AWS Glue Data Catalog
- SQL Query Transform
- Inner Join
- Data Filtering
- Amazon Athena
- Amazon S3
- Parquet
- Snappy Compression

---

# Author

**Abhishek Kumar**


AWS Data Engineering Project – STEDI Human Balance Analytics
