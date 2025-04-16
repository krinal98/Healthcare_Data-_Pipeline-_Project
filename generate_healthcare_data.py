import pandas as pd
import boto3
import random
from faker import Faker  # Correctly importing from the faker library
from io import StringIO

# Initialize Faker for generating fake data
fake = Faker()

# AWS S3 Configuration
AWS_ACCESS_KEY = ""# Replace with your AWS Access Key
AWS_SECRET_KEY = ""  # Replace with your AWS Secret Key
AWS_REGION = "us-east-1"          # Replace with your AWS Region (e.g., 'us-east-1')
S3_BUCKET_NAME = "test-rivery-jb"  # Replace with your S3 bucket name
S3_FILE_NAME = "healthcare_monitoring_data.csv"  # Desired file name in S3

# Function to generate random healthcare data
def generate_healthcare_data(num_records):
    data = []
    for _ in range(num_records):
        patient_id = fake.uuid4()
        age = random.randint(18, 90)
        gender = random.choice(['Male', 'Female'])
        timestamp = fake.date_time_this_year()
        heart_rate = random.randint(60, 100)  # Normal resting heart rate
        steps_count = random.randint(1000, 20000)
        calories_burned = random.uniform(50, 800)
        diagnosis = random.choice(['Healthy', 'At Risk', 'Diabetes', 'Hypertension', 'Cardiac Issue'])

        data.append({
            "Patient_ID": patient_id,
            "Age": age,
            "Gender": gender,
            "Timestamp": timestamp,
            "Heart_Rate": heart_rate,
            "Steps_Count": steps_count,
            "Calories_Burned": round(calories_burned, 2),
            "Diagnosis": diagnosis
        })
    return pd.DataFrame(data)

# Function to upload data to S3
def upload_to_s3(df, bucket_name, file_name):
    # Convert DataFrame to CSV
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Initialize S3 client
    s3_client = boto3.client(
        's3',
        aws_access_key_id=AWS_ACCESS_KEY,
        aws_secret_access_key=AWS_SECRET_KEY,
        region_name=AWS_REGION
    )
    
    # Upload CSV to S3
    s3_client.put_object(
        Bucket=bucket_name,
        Key=file_name,
        Body=csv_buffer.getvalue(),
        ContentType='text/csv'
    )
    print(f"File uploaded to S3: s3://{bucket_name}/{file_name}")

# Main Function
def main():
    num_records = 10000  # Number of rows to generate
    print(f"Generating {num_records} rows of healthcare data...")
    healthcare_data = generate_healthcare_data(num_records)
    
    print(f"Uploading data to S3 bucket '{S3_BUCKET_NAME}'...")
    upload_to_s3(healthcare_data, S3_BUCKET_NAME, S3_FILE_NAME)
    print("Data generation and upload complete.")

if __name__ == "__main__":
    main()
