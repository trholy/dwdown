from minio import Minio
from dwdown.notify import Notifier

# Initialize Notifier
notifier = Notifier(
    server_url="your-gotify-sever.com",
    token="your-access-token",
    priority=5,
    secure=False  # Set to True if your MinIO server is HTTPS
)

# Initialize minio client
minio_client = Minio(
    endpoint="your-minio-sever.com",
    access_key="your-access-key",
    secret_key="your-secret-key",
    secure=False  # Set to True if your MinIO server is HTTPS
)

# List all buckets
buckets = minio_client.list_buckets()

status_dict = {}

for bucket in buckets:
    bucket_name = bucket.name
    print(f"Processing bucket: {bucket_name}")

    # List all objects in the bucket
    objects = minio_client.list_objects(bucket_name, recursive=True)

    # Get number of objects in the bucket
    status_dict[bucket_name] = [len([obj.object_name for obj in objects])]

# Send notification
notifier.send_notification(
    message=status_dict,
    script_name="download-VM"
)
