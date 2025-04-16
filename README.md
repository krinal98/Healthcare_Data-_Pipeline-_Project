# Healthcare_Data-_Pipeline-_Project
This project demonstrates an end-to-end data pipeline built for processing and visualizing healthcare data using a multi-cloud architecture. The goal was to automate the flow of data from local sources to cloud storage, transform and merge it efficiently, and deliver actionable insights via dashboards.

🔧 Tools & Technologies Used

Python – for automating data upload and handling

Google Cloud Storage (GCS) – for storing input data

Amazon S3 – for additional cloud data storage

Rivery (Logic River) – for ETL workflows (Extract, Transform, Load)

Google BigQuery – for cloud-based data warehousing

Looker – for data visualization and dashboards

📌 Project Workflow

Data Generation:
I began by generating synthetic or real healthcare data locally. This included patient records, appointments, treatments, and operational metrics.

Data Upload:
A Python script was created to automate the upload of local data to both Google Cloud Storage and Amazon S3. This step enabled secure and efficient transfer to cloud environments.

ETL & Data Integration:
Using Rivery’s Logic River, I designed workflows to extract data from both GCS and S3. These workflows handled data cleaning (e.g., removing duplicates, handling missing values), transformation, and merging into a single, structured dataset.

Data Warehousing:
The transformed data was loaded into Google BigQuery, providing a centralized, scalable platform for running analytical queries.

Visualization:
Finally, I connected Looker to BigQuery and built interactive dashboards. These dashboards visualized key healthcare insights like patient trends, clinic performance, and resource utilization to support management decision-making.

✅ Outcome
Fully automated data pipeline from local to cloud

Clean and consistent dataset merged from multiple sources

Real-time dashboards for improved decision-making

Scalable, reusable, and secure data architecture
