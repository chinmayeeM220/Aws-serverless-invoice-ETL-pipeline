
# 🚀 Event-Driven Invoice Data Processing Pipeline
### *Serverless • Event-Driven • AWS Native*

<p align="center">
  <img src="./WhatsApp Image 2026-06-17 at 17.40.21.jpeg" alt="Architecture Diagram" width="900"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/AWS-S3-orange?style=flat-square&logo=amazons3" alt="AWS S3"/>
  <img src="https://img.shields.io/badge/AWS-SQS-red?style=flat-square&logo=amazonsqs" alt="AWS SQS"/>
  <img src="https://img.shields.io/badge/AWS-Lambda-orange?style=flat-square&logo=awslambda" alt="AWS Lambda"/>
  <img src="https://img.shields.io/badge/AWS-Glue-blueviolet?style=flat-square&logo=amazonaws" alt="AWS Glue"/>
  <img src="https://img.shields.io/badge/AWS-EventBridge-red?style=flat-square&logo=amazonaws" alt="AWS EventBridge"/>
  <img src="https://img.shields.io/badge/Apache-PySpark-blue?style=flat-square&logo=apachespark" alt="PySpark"/>
  <img src="https://img.shields.io/badge/AWS-CloudWatch-8C1A29?style=flat-square&logo=amazoncloudwatch" alt="AWS CloudWatch"/>
  <img src="https://img.shields.io/badge/AWS-IAM-232F3E?style=flat-square&logo=amazonaws" alt="AWS IAM"/>
</p>

---

## 📌 Overview
A fully serverless, event-driven invoice processing pipeline built on AWS that automates structural text extractions, validation schemas, and analytical formats:
* 📥 **PDF Ingestion:** Automated document landing zones.
* 🧹 **Data Validation & Cleaning:** Multi-stage schema sanity checks.
* ⚙️ **Multi-Stage ETL Transformation:** High-throughput big data orchestration.
* 📦 **Analytics-Ready Storage:** Columnar optimized data warehouse components.

This system simulates real-world enterprise data engineering workflows used in high-volume financial billing and transaction ecosystems.

---

## 📺 Project Walkthrough & Live Demo
Click the link below to watch a 2-minute live execution and complete architectural walkthrough of this pipeline:
👉 **[Watch the Live Project Video on Loom](https://www.loom.com/share/1c4609e227494e96982374c3c7323386)**

---

## 🏗️ System Architecture & Data Flow

Data moves dynamically across isolated processing components to maintain separation of concerns:

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
