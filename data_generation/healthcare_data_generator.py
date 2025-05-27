# -----------------------------------
# Import required libraries
# -----------------------------------
import pandas as pd
import random
from datetime import datetime, timedelta
import os
from google.cloud import storage


# -----------------------------------
# Configuration
# -----------------------------------
BUCKET_NAME = 'your_bucket_name_here'  # Your GCS bucket
FOLDER_NAME = 'inbound'  # Folder inside bucket to upload to


# -----------------------------------
# Define schema using array structure
# -----------------------------------
fields_array = [
   {"field_name": "PatientID", "type": "string"},
   {"field_name": "Name", "type": "string"},
   {"field_name": "Age", "type": "integer"},
   {"field_name": "Gender", "type": "string"},
   {"field_name": "HospitalID", "type": "string"},
   {"field_name": "Location", "type": "string"},
   {"field_name": "AdmissionDate", "type": "date"},
   {"field_name": "DischargeDate", "type": "date"},
   {"field_name": "Department", "type": "string"},
   {"field_name": "Vitals_BP", "type": "string"},       # Flattened nested field
   {"field_name": "Vitals_Pulse", "type": "integer"},   # Flattened nested field
]


# -----------------------------------
# Function to create a single patient record
# -----------------------------------
def generate_patient_record():
   record = {}


   for field in fields_array:
       if field['type'] == 'string':
           if field['field_name'] == 'Name':
               record[field['field_name']] = random.choice(['John Doe', 'Alice Smith', 'Raj Patel', 'Emily Wang'])
           elif field['field_name'] == 'Gender':
               record[field['field_name']] = random.choice(['Male', 'Female', 'Other'])
           elif field['field_name'] == 'HospitalID':
               record[field['field_name']] = random.choice(['HOSP1001', 'HOSP1002', 'HOSP1003'])
           elif field['field_name'] == 'Location':
               record[field['field_name']] = random.choice(['New York', 'San Francisco', 'Chicago'])
           elif field['field_name'] == 'Department':
               record[field['field_name']] = random.choice(['Cardiology', 'Neurology', 'Pediatrics', 'Oncology'])
           elif field['field_name'] == 'Vitals_BP':
               record[field['field_name']] = f"{random.randint(90, 130)}/{random.randint(60, 90)}"
           else:
               record[field['field_name']] = f"{field['field_name']}_{random.randint(1000, 9999)}"


       elif field['type'] == 'integer':
           if field['field_name'] == 'Vitals_Pulse':
               record[field['field_name']] = random.randint(60, 100)
           else:
               record[field['field_name']] = random.randint(1, 100)


       elif field['type'] == 'date':
           random_days = random.randint(1, 1000)
           date_value = datetime.now() - timedelta(days=random_days)
           record[field['field_name']] = date_value.strftime('%Y-%m-%d')


   # Add partition and timestamps
   now = datetime.now()
   record['record_created_date'] = now.strftime('%Y-%m-%d')  # Partitioning column
   record['last_updated_timestamp'] = now.strftime('%Y-%m-%d %H:%M:%S')


   return record


# -----------------------------------
# Upload a file to GCS
# -----------------------------------
def upload_to_gcs(local_file_path, bucket_name, folder_name, destination_blob_name):
   client = storage.Client()
   bucket = client.bucket(bucket_name)
   blob = bucket.blob(f"{folder_name}/{destination_blob_name}")
   blob.upload_from_filename(local_file_path)
   print(f"Uploaded: gs://{bucket_name}/{folder_name}/{destination_blob_name}")


# -----------------------------------
# Main Execution
# -----------------------------------
def main():
   print("Generating patient records...")
   records = [generate_patient_record() for _ in range(1000)]
   df = pd.DataFrame(records)


   timestamp = datetime.now().strftime("%Y-%m-%d-%H")
   filename = f"healthcare_patients_{timestamp}.csv"
   local_path = f"/tmp/{filename}"


   df.to_csv(local_path, index=False)
   print(f"Saved CSV locally: {local_path}")


   upload_to_gcs(local_path, BUCKET_NAME, FOLDER_NAME, filename)


   os.remove(local_path)
   print("Local file removed.")


# -----------------------------------
# Entry Point
# -----------------------------------
if __name__ == "__main__":
   main()

# -------------------------------------------------------------------------------
# Explanation:
# - Uses an array of dicts to define field names and types, improving scalability.
# - Generates patient records with consistent formatting.
# - Adds partitioning and timestamp fields.
# - Saves file with a dynamic name and uploads to GCS bucket.
# - Deletes the file after upload to maintain local hygiene.
# --------------------------------------------------------------------------------
