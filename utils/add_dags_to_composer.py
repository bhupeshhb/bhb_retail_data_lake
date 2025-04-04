import argparse
import glob
import os
import tempfile
from shutil import copytree, ignore_patterns
from google.cloud import storage

def _create_file_list(directory: str, name_replacement: str) -> tuple[str, list[str]]:
    """Copies relevant files to a temporary directory and returns the list."""
    if not os.path.exists(directory):
        print(f"⚠️ Warning: Directory '{directory}' does not exist. Skipping upload.")
        return "", []

    temp_dir = tempfile.mkdtemp()
    files_to_ignore = ignore_patterns("__init__.py", "*_test.py", "*.md")
    copytree(directory, temp_dir, ignore=files_to_ignore, dirs_exist_ok=True)

    files = [
        f for f in glob.glob(f"{temp_dir}/**", recursive=True)
        if os.path.isfile(f)
    ]
    return temp_dir, files

def upload_to_composer(directory: str, bucket_name: str, name_replacement: str) -> None:
    """Uploads DAGs or Data files to Composer's Cloud Storage bucket."""
    temp_dir, files = _create_file_list(directory, name_replacement)

    if not files:
        print(f"⚠️ No files found in '{directory}'. Skipping upload.")
        return

    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    for file in files:
        rel_path = file.replace(f"{temp_dir}/", "").lstrip("/")
        file_gcs_path = os.path.join(name_replacement, rel_path)

        try:
            blob = bucket.blob(file_gcs_path)
            blob.upload_from_filename(file)
            print(f"✅ Uploaded: {file} → gs://{bucket_name}/{file_gcs_path}")
        except Exception as e:
            print(f"❌ Failed to upload {file}: {str(e)}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Upload DAGs and Data to Composer GCS Bucket")
    parser.add_argument("--dags_directory", help="Local path to DAGs folder")
    parser.add_argument("--dags_bucket", help="GCS bucket for Composer environment")
    parser.add_argument("--data_directory", help="Local path to supporting data folder")

    args = parser.parse_args()

    print(f"🧠 DAG Directory: {args.dags_directory}")
    print(f"🪣 GCS Bucket: {args.dags_bucket}")
    print(f"📦 Data Directory: {args.data_directory}")

    if args.dags_directory and os.path.exists(args.dags_directory):
        upload_to_composer(args.dags_directory, args.dags_bucket, "dags/")
    else:
        print("⚠️ Skipping DAGs upload — invalid or missing directory.")

    if args.data_directory and os.path.exists(args.data_directory):
        upload_to_composer(args.data_directory, args.dags_bucket, "data/")
    else:
        print("⚠️ Skipping Data upload — invalid or missing directory.")
