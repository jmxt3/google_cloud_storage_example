# Google Cloud Storage Python Utility

A comprehensive Python utility for managing files and data in Google Cloud Storage (GCS). This script provides a complete set of functions for bucket management, file operations, and metadata handling.

## Overview

`main.py` is a feature-rich Google Cloud Storage client that simplifies common GCS operations through easy-to-use Python functions. It's designed for developers who need to interact with GCS buckets programmatically, whether for data processing, file management, or cloud storage automation.

## Features

### Bucket Management
- **Create buckets** with error handling for existing buckets
- **List all buckets** in your GCP project

### File Upload Operations
- **Upload local files** to GCS with custom metadata
- **Upload string content** directly as blobs
- **Batch upload** multiple files from directories
- **Custom metadata** support for all uploads

### File Download Operations
- **Download blobs** to local files
- **Download blob content** as strings for in-memory processing
- Error handling for missing files

### File Listing and Discovery
- **List blobs** with prefix filtering (simulate folder structure)
- **Metadata-based file search** to find files by custom attributes
- **Blob information** including size, timestamps, and metadata

### Metadata Management
- **View complete blob metadata** (size, content type, hashes, timestamps)
- **Update custom metadata** without re-uploading files
- **Search files by metadata** attributes

### File Management
- **Delete blobs** with error handling
- **Content type detection** for uploads

## Prerequisites

1. **Google Cloud Project** with Cloud Storage API enabled
2. **Authentication** set up via one of:
   - Service account JSON key file
   - Application Default Credentials (ADC)
   - Google Cloud SDK authentication

## Setup

1. **Install dependencies:**
   ```bash
   pip install google-cloud-storage python-dotenv
   ```

2. **Set up authentication:**
   - Create a `.env` file in the project root
   - Add your credentials path:
     ```
     GOOGLE_APPLICATION_CREDENTIALS=/path/to/your/service-account-key.json
     ```
   - Or use Application Default Credentials (recommended for local development)

3. **Configure your GCP project** (if needed in the script)

## Usage Examples

### Basic Bucket Operations
```python
# Create a new bucket
bucket = create_bucket("your-unique-bucket-name")

# List all buckets
list_buckets()
```

### File Upload Examples
```python
# Upload a local file with metadata
bucket_name = "your-bucket-name"
metadata = {"source": "script", "version": "1.0", "processed": "false"}
upload_blob(bucket_name, "local_file.txt", "remote/path/file.txt", metadata)

# Upload string content
content = "This is my text content"
upload_string_as_blob(bucket_name, content, "generated/file.txt", "text/plain")

# Batch upload from directory
for filename in os.listdir("./local_files/"):
    if filename.endswith(".txt"):
        local_path = os.path.join("./local_files/", filename)
        remote_path = f"uploads/{filename}"
        upload_blob(bucket_name, local_path, remote_path)
```

### File Download Examples
```python
# Download to local file
download_blob(bucket_name, "remote/file.txt", "local_copy.txt")

# Download as string for processing
content = download_blob_as_string(bucket_name, "remote/file.txt")
if content:
    print(f"File content: {content[:100]}...")
```

### Metadata Operations
```python
# View file metadata
blob_info = get_blob_metadata(bucket_name, "remote/file.txt")

# Update metadata
new_metadata = {"processed": "true", "reviewed_by": "user123"}
update_blob_metadata(bucket_name, "remote/file.txt", new_metadata)

# Find files by metadata
matching_files = find_files_by_metadata(
    bucket_name,
    prefix="uploads/",
    metadata_filter={"processed": "false"}
)
```

### File Listing and Management
```python
# List all files in bucket
list_blobs(bucket_name)

# List files with prefix (simulate folders)
list_blobs(bucket_name, prefix="uploads/")

# Delete a file
delete_blob(bucket_name, "remote/file.txt")
```

## Key Functions

| Function | Description |
|----------|-------------|
| `create_bucket()` | Creates a new GCS bucket |
| `list_buckets()` | Lists all buckets in the project |
| `upload_blob()` | Uploads a file with optional metadata |
| `upload_string_as_blob()` | Uploads string content as a blob |
| `download_blob()` | Downloads a blob to a local file |
| `download_blob_as_string()` | Downloads blob content as a string |
| `list_blobs()` | Lists blobs with optional prefix filtering |
| `get_blob_metadata()` | Retrieves comprehensive blob metadata |
| `update_blob_metadata()` | Updates custom metadata |
| `delete_blob()` | Deletes a blob from the bucket |
| `find_files_by_metadata()` | Searches files by custom metadata |

## Error Handling

The script includes comprehensive error handling for common scenarios:
- **Bucket already exists** (409 Conflict)
- **File not found** during downloads
- **Permission errors**
- **Network issues**

## Security Notes

- Store credentials securely using environment variables
- Never commit service account keys to version control
- Use IAM roles with minimal required permissions
- Consider using Application Default Credentials for local development

## Logging

The script uses Python's logging module for informational messages. Logging level is set to INFO by default.

## Customization

The script is designed to be easily modified for specific use cases:
- Add custom validation for uploads
- Implement retry logic for failed operations
- Add progress tracking for large file operations
- Extend metadata schemas for your application needs

## Dependencies

- `google-cloud-storage`: Official Google Cloud Storage client library
- `python-dotenv`: Environment variable management
- `os`, `logging`: Standard Python libraries
