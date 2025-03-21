# Real-Time E-Commerce ETL Pipeline

A real-time ETL and analytics pipeline built for e-commerce platforms. This project simulates ingestion of transactional and product data, performs transformations using Apache Spark, and loads the results into Amazon Redshift for analysis via QuickSight or Tableau.

## 🔧 Tech Stack

- **AWS Kinesis** – Real-time data ingestion
- **Amazon S3** – Raw and backup data storage
- **Apache Spark (PySpark)** – Data transformation
- **Amazon Redshift** – Data warehouse for analytics
- **Apache Airflow** – Workflow orchestration
- **Amazon QuickSight / Tableau** – BI and dashboards
- **Terraform** – Infrastructure as code
- **GitHub Actions** – CI/CD for automated deployment

## 🚀 Features

- Simulates realistic e-commerce transactions with sample data
- Real-time ingestion and batch processing capabilities
- Cleans, transforms, and enriches data using PySpark
- Automatically loads processed data into Redshift
- Enables reporting on customer behavior, sales trends, inventory, and promotions
- Includes monitoring and alerting with AWS CloudWatch

## 📁 Folder Structure

```bash
.
├── airflow/                    # Airflow setup
├── spark/                      # PySpark jobs and configs
├── data_generation/            # Sample data generators
├── redshift/                   # Redshift setup scripts
├── config/                     # Global/shared config
├── tests/                      # Pytest unit/integration tests
├── dashboards/                 # (Optional) Dashboard exports or screenshots
├── docker/                     # Docker configs per service
├── .env                        # Environment variables
├── docker-compose.yml          # Main Docker setup
├── requirements.txt            # Global project dependencies
└── README.md                   # Project overview and instructions

```

## 🧪 Sample Data

Run the provided script to generate fake but realistic data for:
- Customers, Orders, Products, Payments, Shipments, Promotions, Inventory, and more

```bash
python generate_sample_data.py
```

## ⚙️ Setup & Deployment

1. Clone the repo:

```bash
git clone https://github.com/your-username/real-time-ecommerce-etl-pipeline.git
cd real-time-ecommerce-etl-pipeline
```

2. Create and activate your virtual environment:

```bash
python -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

3. Provision AWS infrastructure (optional):

```bash
cd terraform
terraform init
terraform apply
```

4. Run your ETL job locally or deploy to EMR / Airflow.

## 📊 Dashboard Preview

<screenshot or link to dashboard or analytics UI here>

## 🛠 Future Enhancements

- Integrate ML model for product recommendations
- Add user notification system using AWS SES
- Extend to multi-cloud (GCP or Azure)
- Add Kafka support for ingestion

## 📄 License

MIT License. See `LICENSE` for more information.

