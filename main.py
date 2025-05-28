import os
import logging
from dotenv import load_dotenv
from google.cloud import storage
load_dotenv()  # Load environment variables from .env file
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

GOOGLE_APPLICATION_CREDENTIALS = os.getenv('GOOGLE_APPLICATION_CREDENTIALS')

# If GOOGLE_APPLICATION_CREDENTIALS is set, the client will use it.
# Otherwise, it will try to use Application Default Credentials.
# storage_client = storage.Client.from_service_account_json(GOOGLE_APPLICATION_CREDENTIALS)
storage_client = storage.Client()

# You can also explicitly pass project ID if needed, though often not necessary
# if credentials are set up correctly.
# storage_client = storage.Client(project='your-gcp-project-id')

def create_bucket(bucket_name):
    """Creates a new bucket."""
    try:
        bucket = storage_client.create_bucket(bucket_name)
        print(f"Bucket {bucket.name} created.")
        return bucket
    except Exception as e:
        print(f"Error creating bucket {bucket_name}: {e}")
        # Handle bucket already exists, permissions, etc.
        if "409" in str(e): # 409 Conflict usually means it already exists
            print(f"Bucket {bucket_name} likely already exists.")
            return storage_client.bucket(bucket_name)
        return None

def list_buckets():
    """Lists all buckets."""
    buckets = storage_client.list_buckets()
    print("Buckets:")
    for bucket in buckets:
        print(bucket.name)
    return buckets

# Example usage:
# my_bucket_name = "your-unique-bucket-name-for-txt-files" # MUST BE GLOBALLY UNIQUE
# text_files_bucket = create_bucket(my_bucket_name)
# logger.info(f"Created bucket: {text_files_bucket.name if text_files_bucket else 'None'}")
# list_buckets()

def upload_blob(bucket_name, source_file_name, destination_blob_name, custom_metadata=None):
    """Uploads a file to the bucket with optional custom metadata."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # Set custom metadata if provided
        if custom_metadata:
            blob.metadata = custom_metadata

        blob.upload_from_filename(source_file_name)
        print(f"File {source_file_name} uploaded to {destination_blob_name}.")
        print(f"  Content Type: {blob.content_type}")
        if blob.metadata:
            print(f"  Custom Metadata: {blob.metadata}")
        return blob
    except Exception as e:
        print(f"Error uploading {source_file_name} to {destination_blob_name}: {e}")
        return None

def upload_string_as_blob(bucket_name, text_content, destination_blob_name, content_type="text/plain", custom_metadata=None):
    """Uploads a string content to the bucket as a blob."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        if custom_metadata:
            blob.metadata = custom_metadata

        blob.upload_from_string(text_content, content_type=content_type)
        print(f"String content uploaded to {destination_blob_name} in bucket {bucket_name}.")
        if blob.metadata:
            print(f"  Custom Metadata: {blob.metadata}")
        return blob
    except Exception as e:
        print(f"Error uploading string to {destination_blob_name}: {e}")
        return None

# Example: Uploading a single TXT file
# bucket_name = "your-unique-bucket-name" # Make sure this bucket exists
# local_file_path = "local_file.txt"
# gcs_object_name = "my_texts/uploads/local_file.txt" # Simulates folder structure

# Create a dummy local file for testing
# with open(local_file_path, "w") as f:
#     f.write("This is a test TXT file.")

# my_custom_metadata = {"source": "script", "version": "1.0", "processed": "false"}
# upload_blob(bucket_name, local_file_path, gcs_object_name, custom_metadata=my_custom_metadata)

# Example: Uploading many local TXT files from a directory
# source_directory = "./my_local_txt_files/" # Ensure this directory exists with TXT files
# if not os.path.exists(source_directory):
#     os.makedirs(source_directory)
#     with open(os.path.join(source_directory, "file1.txt"), "w") as f: f.write("Content of file1")
#     with open(os.path.join(source_directory, "file2.txt"), "w") as f: f.write("Content of file2")


# for filename in os.listdir(source_directory):
#     if filename.endswith(".txt"):
#         local_path = os.path.join(source_directory, filename)
#         # Example: store in GCS with a prefix like 'raw_files/'
#         destination_path = f"raw_files/{filename}"
#         file_metadata = {"original_filename": filename, "upload_batch": "initial_load"}
#         upload_blob(bucket_name, local_path, destination_path, custom_metadata=file_metadata)

# Example: Uploading generated text content
# generated_content = "This is dynamically generated text for file3."
# destination_blob_name_generated = "generated_texts/file3.txt"
# metadata_for_generated = {"type": "generated", "status": "new"}
# upload_string_as_blob(bucket_name, generated_content, destination_blob_name_generated, custom_metadata=metadata_for_generated)


def list_blobs(bucket_name, prefix=None, delimiter=None):
    """Lists blobs in the bucket, optionally filtering by prefix and delimiter."""
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix, delimiter=delimiter)

    print(f"Blobs in gs://{bucket_name}/{prefix or ''}:")
    for blob in blobs:
        print(f"  - {blob.name} (Size: {blob.size} bytes, Updated: {blob.updated})")
        if blob.metadata:
            print(f"    Custom Metadata: {blob.metadata}")

    # If a delimiter is used, list_blobs() will also return "prefixes"
    # that represent subdirectories.
    if delimiter:
        print("Subdirectories (prefixes):")
        # The `prefixes` attribute is only available on the `BlobsPageIterator`
        # returned when `delimiter` is used. You might need to access it via `iterator.prefixes`.
        # This part might need adjustment based on how you iterate if you're expecting prefixes.
        # A common way is:
        # iterator = bucket.list_blobs(prefix=prefix, delimiter=delimiter)
        # for page in iterator.pages:
        #     for p in page.prefixes:
        #         print(f"  - {p}")
        # This example keeps it simpler by just iterating blobs.
        # If you need to handle prefixes (subdirectories), you'll need to check the API
        # documentation for the specific way the client library version handles it.
        # For now, we'll focus on the blobs themselves.
        pass # Add prefix handling if needed for directory-like listing.


# Example usage:
# bucket_name = "your-unique-bucket-name"
# list_blobs(bucket_name)
# list_blobs(bucket_name, prefix="my_texts/uploads/") # List files in 'my_texts/uploads/' "folder"
# list_blobs(bucket_name, prefix="my_texts/", delimiter="/") # List files and "subfolders" in 'my_texts/'

def download_blob(bucket_name, source_blob_name, destination_file_name):
    """Downloads a blob from the bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        blob.download_to_filename(destination_file_name)
        print(f"Blob {source_blob_name} downloaded to {destination_file_name}.")
        return True
    except Exception as e:
        print(f"Error downloading {source_blob_name}: {e}")
        # Handle file not found (google.cloud.exceptions.NotFound)
        return False

def download_blob_as_string(bucket_name, source_blob_name):
    """Downloads a blob's content as a string."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(source_blob_name)
        content = blob.download_as_text() # Or download_as_bytes() for binary
        print(f"Content of {source_blob_name} retrieved.")
        return content
    except Exception as e:
        print(f"Error downloading {source_blob_name} as string: {e}")
        return None

# Example usage:
# bucket_name = "your-unique-bucket-name"
# gcs_object_to_download = "my_texts/uploads/local_file.txt"
# local_download_path = "downloaded_file.txt"
# download_blob(bucket_name, gcs_object_to_download, local_download_path)

# content_string = download_blob_as_string(bucket_name, gcs_object_to_download)
# if content_string:
#     print(f"Content:\n{content_string[:100]}...") # Print first 100 chars

def get_blob_metadata(bucket_name, blob_name):
    """Gets a blob's metadata."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.get_blob(blob_name)

        if not blob:
            print(f"Blob {blob_name} not found in bucket {bucket_name}.")
            return None

        print(f"Metadata for {blob.name}:")
        print(f"  Bucket: {blob.bucket.name}")
        print(f"  Name: {blob.name}")
        print(f"  Size: {blob.size} bytes")
        print(f"  Content Type: {blob.content_type}")
        print(f"  Created: {blob.time_created}")
        print(f"  Updated: {blob.updated}")
        print(f"  MD5 Hash: {blob.md5_hash}")
        print(f"  CRC32c: {blob.crc32c}")
        print(f"  Generation: {blob.generation}")
        print(f"  Metageneration: {blob.metageneration}")
        if blob.metadata: # Custom metadata
            print("  Custom Metadata:")
            for key, value in blob.metadata.items():
                print(f"    {key}: {value}")
        else:
            print("  No custom metadata.")
        return blob
    except Exception as e:
        print(f"Error getting metadata for {blob_name}: {e}")
        return None

# Example usage:
# bucket_name = "your-unique-bucket-name"
# gcs_object_for_metadata = "my_texts/uploads/local_file.txt"
# blob_details = get_blob_metadata(bucket_name, gcs_object_for_metadata)
# if blob_details and blob_details.metadata and 'source' in blob_details.metadata:
#     print(f"Custom metadata 'source': {blob_details.metadata['source']}")

def update_blob_metadata(bucket_name, blob_name, new_metadata):
    """Updates a blob's custom metadata. This will overwrite existing custom metadata."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.metadata = new_metadata
        blob.patch() # Use patch() to update metadata without re-uploading the file
        print(f"Metadata updated for blob {blob_name}.")
        print(f"New custom metadata: {blob.metadata}")
        return blob
    except Exception as e:
        print(f"Error updating metadata for {blob_name}: {e}")
        return None

# Example usage:
# bucket_name = "your-unique-bucket-name"
# gcs_object_to_update = "my_texts/uploads/local_file.txt"
# updated_meta = {"processed": "true", "reviewed_by": "user_x", "priority": "high"}
# update_blob_metadata(bucket_name, gcs_object_to_update, updated_meta)
# get_blob_metadata(bucket_name, gcs_object_to_update) # Verify

def delete_blob(bucket_name, blob_name):
    """Deletes a blob from the bucket."""
    try:
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(blob_name)
        blob.delete()
        print(f"Blob {blob_name} deleted.")
        return True
    except Exception as e:
        print(f"Error deleting {blob_name}: {e}")
        # Handle file not found (google.cloud.exceptions.NotFound)
        return False

# Example usage:
# delete_blob(bucket_name, "my_texts/uploads/local_file.txt")

def find_files_by_metadata(bucket_name, prefix=None, metadata_filter=None):
    """
    Lists blobs and filters them by custom metadata in Python.
    metadata_filter should be a dict like {"key1": "value1", "key2": "value2"}
    """
    bucket = storage_client.bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    found_files = []

    for blob in blobs:
        if metadata_filter and blob.metadata:
            match = True
            for key, value in metadata_filter.items():
                if not (key in blob.metadata and blob.metadata[key] == value):
                    match = False
                    break
            if match:
                found_files.append(blob)
        elif not metadata_filter: # No filter, add all (though usually you'd want a filter)
            found_files.append(blob)

    print(f"Found {len(found_files)} files matching filter {metadata_filter} with prefix '{prefix or ''}':")
    for blob in found_files:
        print(f"  - {blob.name}, Metadata: {blob.metadata}")
    return found_files

# Example:
# bucket_name = "your-unique-bucket-name"
# find_files_by_metadata(bucket_name,
#                        prefix="raw_files/",
#                        metadata_filter={"upload_batch": "initial_load"})
# find_files_by_metadata(bucket_name, metadata_filter={"status": "new"})