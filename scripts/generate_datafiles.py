import uuid
import pyarrow as pa
import pyarrow.parquet as pq
import s3fs
import random
from datetime import datetime

# MinIO connection settings
MINIO_ENDPOINT = "http://localhost:9000"
AWS_ACCESS_KEY_ID = "admin"
AWS_SECRET_ACCESS_KEY = "password"
S3_BUCKET = "warehouse"
BASE_PATH = f"s3://{S3_BUCKET}/data"

# Number of tenants
total_tenants = 20
tenants = [f"tenant{i+1}" for i in range(total_tenants)]

schema = pa.schema([
    ("tenant", pa.string()),
    ("id", pa.int32()),
    ("name", pa.string()),
    ("amount", pa.float64()),
    ("event_time", pa.timestamp("ms")),
])

def generate_data(tenant, num_rows=100):
    data = {
        "tenant": [tenant for _ in range(num_rows)],
        "id": [random.randint(1, 1000) for _ in range(num_rows)],
        "name": [random.choice(["Alice", "Bob", "Charlie", "David"]) for _ in range(num_rows)],
        "amount": [round(random.uniform(10.0, 500.0), 2) for _ in range(num_rows)],
        "event_time": [datetime.now() for _ in range(num_rows)]
    }
    return pa.Table.from_pydict(data, schema=schema)

# Initialize S3 filesystem for MinIO
fs = s3fs.S3FileSystem(
    key=AWS_ACCESS_KEY_ID,
    secret=AWS_SECRET_ACCESS_KEY,
    client_kwargs={"endpoint_url": MINIO_ENDPOINT},
)

# Generate and write Parquet files for each tenant
for tenant in tenants:
    tenant_path = f"{BASE_PATH}/{tenant}/"
    for i in range(2):
        file_path = f"{tenant_path}data{i + 1}_{uuid.uuid4().__str__()}.parquet"
        table = generate_data(tenant, 100)
        with fs.open(file_path, "wb") as f:
            pq.write_table(table, f)
        print(f"Written: {file_path}")

print("Parquet file generation completed.")
