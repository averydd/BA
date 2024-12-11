import os

# MongoDB Configuration
MONGO_URI = os.getenv(
    "MONGO_URI",
    "mongodb://dsReader:ds_reader_ndFwBkv3LsZYjtUS@178.128.85.210:27017,104.248.148.66:27017,103.253.146.224:27017"
)

# PostgreSQL Configuration
POSTGRES_URI = os.getenv(
    "POSTGRES_URI",
    "postgresql://dsreader:ds_reader_ndfwbkv3lszyjtus@152.42.187.199:5432/postgres"
)
TARGET_POSTGRES_URI = os.getenv(
    "TARGET_POSTGRES_URI", "postgresql://target_user:target_pass@localhost:5432/target_db"
)
