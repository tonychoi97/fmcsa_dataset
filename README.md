## Running fmcsa_dataset with Docker.

Run docker containers:

```
docker compose up
```

Build the Docker image with the following prompt:

```
Docker build -t fmcsa_test_image .
```

Run the image with the arguments below:

```
URL="https://ai.fmcsa.dot.gov/SMS/files/FMCSA_CENSUS1_2025Jun.zip" \

docker run -it \
  --network=fmcsa_dataset_default \
  fmcsa_test_image \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=fmcsa_db \
    --table_name=fmcsa_records \
    --url=${URL}
```

docker run -it \
-e POSTGRES_USER="root" \
-e POSTGRES_PASSWORD="root" \
-e POSTGRES_DB="act_expo" \
-v c:/Users/Jaewo/git/fmcsa_dataset/fmcsa_datasets:/var/lib/postgresql/data \
-p 5432:5432 \
postgres:13

C:\Users\Jaewo\git\fmcsa_dataset\fmcsa_datasets