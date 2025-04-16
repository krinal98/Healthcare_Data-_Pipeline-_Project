import os
import pandas as pd
import random
from faker import Faker
from io import StringIO
from google.cloud import storage  # GCP storage library

# Initialize Faker for generating fake data
fake = Faker()

# GCP Configuration
GCP_BUCKET_NAME = "rivery_gcs_krinal"  # GCP bucket name
GCP_FILE_NAME = "healthcare_monitoring_data.csv"  # GCS file name
GCP_CREDENTIALS_FILE = "data-pipeline1-36ce067e4330.json"  # Path to credentials file

# Function to generate random healthcare data
def generate_healthcare_data(num_records):
    data = []
    for _ in range(num_records):
        patient_id = fake.uuid4()
        age = random.randint(18, 90)
        gender = random.choice(['Male', 'Female'])
        timestamp = fake.date_time_this_year()
        heart_rate = random.randint(60, 100)
        steps_count = random.randint(1000, 20000)
        calories_burned = round(random.uniform(50, 800), 2)
        diagnosis = random.choice(['Healthy', 'At Risk', 'Diabetes', 'Hypertension', 'Cardiac Issue'])
        data.append({
            "Patient_ID": patient_id,
            "Age": age,
            "Gender": gender,
            "Timestamp": timestamp,
            "Heart_Rate": heart_rate,
            "Steps_Count": steps_count,
            "Calories_Burned": calories_burned,
            "Diagnosis": diagnosis
        })
    return pd.DataFrame(data)

# Function to upload data to GCP Storage
def upload_to_gcs(df, bucket_name, file_name):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    csv_buffer.seek(0)
    storage_client = storage.Client.from_service_account_json(GCP_CREDENTIALS_FILE)
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(file_name)
    blob.upload_from_file(csv_buffer, content_type='text/csv')
    print(f"File uploaded to GCS: gs://{bucket_name}/{file_name}")

# Main function
def main():
    try:
        num_records = 10000
        print(f"Generating {num_records} rows of healthcare data...")
        healthcare_data = generate_healthcare_data(num_records)
        print(f"Uploading data to GCS bucket '{GCP_BUCKET_NAME}'...")
        upload_to_gcs(healthcare_data, GCP_BUCKET_NAME, GCP_FILE_NAME)
        print("Data generation and upload to GCS complete.")
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
