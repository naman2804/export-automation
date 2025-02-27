import os
import json
import threading
from zipfile import ZipFile

from cloudevents.http import from_http
from flask import Flask, request
from google.cloud import storage

app = Flask(__name__)
storage_client = storage.Client()

def process_event(event):
    """Processes the Cloud Storage event in a separate thread."""
    try:
        print("In container")
        print(event)
        bucket = event.get("subject")
        print(f"Bucket: {bucket}")
        event_type = event.get("type")

        if event_type == "google.cloud.storage.object.v1.finalized" and "export_complete" in bucket:
            print(f"Detected creation of export_complete.txt in: {bucket}")

            try:
                object_name = event["source"]
                path = event.get("subject")
                bucket_name = object_name.split('/buckets/')[1]
                date_path = '/'.join(path.split('/')[1:4])  # Extract 'YYYY/MM/DD'
                file_path = f"{bucket_name}/{date_path}"
                print(f"File Path: {file_path}")

                bucket_name1, folder_path = file_path.split("/", 1) 
                folder_path += "/"
                print(bucket_name1)
                print(folder_path)
                """
                x = download_and_zip_files(
                    source_bucket_name=bucket_name1,
                    prefix=folder_path,
                    destination_bucket_name='pyze-automation-dev3-bucket'
                )
                #print(x)
                """
            except Exception as e:
                print(f"Inner error: {e}")
        else:
            print("Export complete not present")
    except Exception as e:
        print(f"Error processing event: {str(e)}")

""""
def download_and_zip_files(source_bucket_name, prefix, destination_bucket_name, destination_folder='./'):
    print("Inside download function")
    try:
        source_bucket = storage_client.get_bucket(source_bucket_name)
        destination_bucket = storage_client.get_bucket(destination_bucket_name)
        print(source_bucket_name)
        print(prefix)

        blobs = storage_client.list_blobs(source_bucket_name, prefix=prefix)
        files_to_zip = [blob for blob in blobs if not blob.name.endswith('/')]

        if not files_to_zip:
            print("No files found to zip.")
            return None

        zip_filename = os.path.join(destination_folder, f'{prefix.strip("/").replace("/", "_")}.zip')
        with ZipFile(zip_filename, 'w') as zipf:
            for blob in files_to_zip:
                print(f"Downloading and adding {blob.name} to the zip file...")
                data = blob.download_as_bytes()
                zipf.writestr(os.path.basename(blob.name), data)

        print(f'All files zipped successfully into {zip_filename}')

        destination_blob_name = f'{prefix.strip("/")}.zip'
        blob = destination_bucket.blob(destination_blob_name)
        blob.upload_from_filename(zip_filename)
        print(f'Zip file uploaded back to GCS at gs://{destination_bucket_name}/{destination_blob_name}')

        return f'gs://{destination_bucket_name}/{destination_blob_name}'
    
    except Exception as e:
        print(f"An error occurred: {e}")
        return None
"""
@app.route("/", methods=["POST"])
def index():
    """Handles incoming HTTP requests and starts processing in a separate thread."""
    try:
        event = from_http(request.headers, request.data)
        print("before thread")
        threading.Thread(target=process_event, args=(event,)).start()
        print("sending 200 ok")
        return "Event received, processing in background", 200

    except Exception as e:
        print(f"Error processing event: {str(e)}")
        return "Internal server error", 500

if __name__ == "__main__":
    app.run(debug=True, host="0.0.0.0", port=int(os.environ.get("PORT", 8080)))
