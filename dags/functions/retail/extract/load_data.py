import os
import requests

from functions.utils import get_s3_client


def download_file(url, output_path):
    try:
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(output_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        print(f"✅ Downloaded {url}")
        return True
    except Exception as e:
        print(f"❌ Failed to download {url}: {e}")
        return False


def upload_to_minio(file_path, bucket_name, object_name):
    s3 = get_s3_client()
    try:
        s3.upload_file(file_path, bucket_name, object_name)
        print(f"⬆️  Uploaded to MinIO: {object_name}")
    except Exception as e:
        print(f"❌ Failed to upload {object_name} to MinIO: {e}")


def data_ingestion():
    file_urls = [
        f'https://tdhghaslnufgtzjybhhf.supabase.co/storage/v1/object/public/datasets/Retail/retail_data_{i}.csv' for i in range(1, 10+1)]

    bucket_name = "etl-dag"
    s3 = get_s3_client()

    s3.head_bucket(Bucket=bucket_name)

    os.makedirs("tmp_downloads", exist_ok=True)

    for url in file_urls:
        filename = url.split("/")[-1]
        local_path = os.path.join("tmp_downloads", filename)

        if download_file(url, local_path):
            upload_to_minio(local_path, bucket_name,
                            f"bronze/data/raw/{filename}")
