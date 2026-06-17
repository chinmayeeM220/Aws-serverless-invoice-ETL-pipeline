# 🚀 Event-Driven Invoice Data Processing Pipeline
### *Serverless • Scalable • Event-Driven • AWS Native*

[![System Architecture](https://img.shields.io/badge/AWS-Architecture-orange?logo=amazon-aws)](https://aws.amazon.com/)
[![Engine](https://img.shields.io/badge/Apache-Spark%20%2F%20PySpark-blue?logo=apachespark)](https://spark.apache.org/)

## 📺 Project Walkthrough & Live Demo
Click the link below to watch a 2-minute live deployment and architectural walkthrough of this pipeline:
👉 **[Watch the Live Project Video on Loom](https://www.loom.com/share/1c4609e227494e96982374c3c7323386)**

---

## 🏗️ System Architecture & Data Flow

This project takes a classic business problem—processing messy, manual PDF invoices—and converts it into an automated, serverless digital assembly line. Data cascades seamlessly through **7 distinct production stages**:

```text
[1. INGESTION]          --> PDFs arrive via Email & drop into 'sqs-itw-input-bucket1'
       ↓
[2. EVENT QUEUING]      --> S3 Event Notification triggers 'Amazon SQS Queue' (Decoupling Layer)
       ↓
[3. PROCESSING STAGE 1] --> 'AWS Lambda 1' wakes up and triggers 'AWS Glue Job 1' (Validation & Cleaning)
       ↓
[4. INTERMEDIATE STRG]  --> Cleaned & Validated JSON metadata is saved to 'sqs-itw-output-bucket2'
       ↓
[5. ORCHESTRATION]      --> 'Amazon EventBridge' catches the "Glue 1 SUCCESS" state and fires 'AWS Lambda 2'
       ↓
[6. PROCESSING STAGE 2] --> 'AWS Lambda 2' spins up 'AWS Glue Job 2' (PySpark Columnar Transformation)
       ↓
[7. FINAL STORAGE]      --> High-performance, Snappy-compressed Parquet warehouse in 'sqs-itw-parquet-bucket'
