# SaaS Customer Data Pipeline

A PySpark-based data enginnering pipeline that ingests customer data from multiple sources, cleans, and standardizes the records, resolves duplicate customer identities, and produces a unified Customer 360 dataset.

The project demonstrates a multi-layer data pipeline architecture using Python and Apache Spark, with a focus on data transformation, data quality, entity resolution, and reliable analytical outputs.

## Project Overview

Customer information is often distributed across multiple systems such as CRM, marketing, and sales platforms. The same customer may appear differently in each system because of differences in names, company names, phone numbers, or other attributes.

This project processes those records through a Bronze → Silver → Gold pipeline and uses entity resolution to determine which records represent the same real-world customer.

## Pipeline Flow

- CRM & Marketing & Sales ->
- Bronze ->
- Silver ->
- Data Quality ->
- Entity Resolution ->
- Match Reports & Entity Groups ->
- Customer 360 ->
- Gold

## Key Features

- Muli-source customer data ingestion
- Bronze, silver, and Gold data layers
- PySpark-based trnasformations
- Data normalization and standaradization
- Email and phone matching
- Name and comapny similarity matching
- Candidate blocking to reduce unnecessary comparisons
- Matching confidence classification
- Trusted entity resolution
- Stable entity IDs for unified customers
- Customer 360 dataset
- Data-quality validation
- JSON reporting
- Pipeline validation and completeness checks

## Technologies

## Technology      Purpose

Python             Pipeline development and orchestration
PySpark 4.2.0      Distributed data processing and transformation
Java 17            Spark runtime
JSON               Source data and pipeline reports
Git/GitHub         Version control and project management

## Architecture

The pipeline is organized into three primary data layers.

## Bronze Layer

The Bronze layer stores the source data with minimal transformation.

CRM & Marketing & Sales
          |
          V
        Bronze

Each source is preserved seperately so that the original records can be traced back to their source.

Example:

data/

└── bronze/ 
    
    ├── crm/ 
    
    ├── marketing/ 
    
    └── sales/

The Bronze layer provides a reproducible starting point for downstream processing.

## Silver Layer

The Silver layer standardizes the records from each source into a common customer schema.

The standardized records contain fields such as:

- customer_name
- email
- company
- phone
- source_id
- source

Source-specific differences are handled during transformation so that records from CRM, Marketing, and Sales can be processed consistently.

The Silver layer also performs data-quality checks including:

- Missing-value checks
- Duplicate email checks
- Invalid email checks
- Field normalization

## Entity Resolution

One of the main challenges in the project is determining when records from different systems represent the same customer.

For example:

CRM:       L Graham
Marketing: Leanne Graham
Sales:     L. Graham

These records have different representations of the customer's name but share identifying information such as email and phone.

The pipeline resolves these records into a single entity.

## 1. Blocking

Comparing every record against every other record would create unnecessary comparisons as the dataset grows.

The pipeline therefore creates candidate groups using blocking attributes before calculating detailed match scores.

This reduces the number of record pairs that need to be evaluated.

Silver Records
      
      │
      ▼
   Blocking
      
      │
      ▼
Candidate Pairs

## 2. Candidate Generation

Potentially related records are generated from the blocked data.

Each candidate pair contains two source records that may represent the same customer.

## 3. Match Scoring

Candidate pairs are scored using multiple customer attributes.

The scoring process considers:

Email match
Phone match
Customer-name similarity
Company similarity

Name and company comparisons use string similarity calculations implemented through Spark transformations.

The resulting match score is used together with identifying fields to classify candidate pairs.

## 4. Match Confidence

Candidate matches are separated into confidence categories:

High Confidence
      
      │
      ├── Strong overall match
      │
Medium Confidence
      
      │
      ├── Partial evidence
      │
No Match
      
      │
      └── Insufficient evidence

Trusted matches are then used to construct entity groups.

## Entity Groups

Each resolved customer receives a stable entity_id.

For example:

C001 ─┐

L001 ─┼──> ENT-a4906a54b9b8

S001 ─┘

This allows records from multiple source systems to be associated with the same real-world customer.

The entity ID becomes the key used by downstream Customer 360 processing.

## Customer 360

After entity resolution, the pipeline combines the records associated with each entity into a unified customer view.

Example:

Entity ID:     ENT-a4906a54b9b8

Customer:      L Graham

Email:         sincere@april.biz

Company:       Romaguera-Crona

Phone:         1-770-736-8031 x56442

Sources:       CRM, Marketing, Sales

Source IDs:    C001, L001, S001

The Customer 360 output provides a single analytical representation of each customer while preserving the source systems from which the information originated.

## Gold Layer

The Gold layer contains the final Customer 360 dataset intended for analytical consumption.

The resulting dataset includes fields such as:

entity_id
customer_name
email
company
phone
source_count
sources
source_ids

This creates a clean, unified customer dataset that downstream analytics applications can consume without having to repeat the entity-resolution process.

## Data Quality

The pipeline performs validation throughout the processing workflow.

Current checks include:

Missing email values
Missing company values
Missing phone values
Duplicate entities
Duplicate emails
Invalid email formats
Customer completeness
Source coverage

The final pipeline reports the number of customers and the percentage of records missing important customer attributes.

Example:

Total customers: 10
Missing email: 0
Missing company: 0
Missing phone: 0

## Reports

Pipeline results and validation information are stored as JSON reports rather than relying on large terminal dumps.

Current reporting includes:

reports/

├── high_confidence_matches.json

├── entity_groups.json

├── data_quality.json

└── pipeline_run.json

These reports make pipeline results easier to inspect, validate, and reuse without rerunning the entire Spark job.

## Project Structure

SaaS Customer Data Pipeline/

│

├── data/

│   ├── raw/

│   ├── bronze/

│   ├── silver/

│   └── gold/

│

├── reports/

│

├── src/

│   ├── ingestion/

│   ├── matching/

│   ├── quality/

│   ├── transformation/

│   ├── check_quality.py

│   ├── customer_360.py

│   ├── import_files.py

│   ├── matching_modules.py

│   ├── transform_data.py

│   └── main.py

│

├── requirements.txt

├── .gitignore

└── README.md

The top-level helper modules are used to keep the main pipeline orchestration concise while the underlying functionality remains separated into logical modules.

## How to Run

### 1. Clone the repository

git clone <repository-url>
cd "SaaS Customer Data Pipeline"

### 2. Create a virtual environment

Windows
python -m venv .venv

Activate it:

.venv\Scripts\activate

### 3. Install dependencies

pip install -r requirements.txt
The project requires Python, PySpark, and Java 17.

### 4. Run the pipeline

python src\main.py

The pipeline will process the source data through the Bronze, Silver, entity-resolution, Customer 360, and Gold stages.

Generated outputs and reports are written to their respective directories.

## Example End-to-End Flow

Source Data

    │
    ▼
┌─────────────┐

│   Bronze    │

│ Raw Records │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│   Silver    │

│ Standardize │

│ + Validate  │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│   Blocking  │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│ Candidates  │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│ Match Score │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│   Entity    │

│ Resolution  │

└──────┬──────┘

       │
       ▼
┌─────────────┐

│ Customer 360│

└──────┬──────┘

       │
       ▼
┌─────────────┐

│    Gold     │

└─────────────┘

## Project Outcome

The completed pipeline demonstrates how customer data from multiple operational systems can be transformed into a unified analytical dataset.

The project focuses on practical data engineering concepts including:

- Batch data processing
- Data transformation
- Data quality
- Entity resolution
- Data modeling
- Pipeline organization
- Reproducible outputs
- Spark-based processing

The final result is a unified Customer 360 dataset that connects records across CRM, Marketing, and Sales systems using a consistent entity identity.