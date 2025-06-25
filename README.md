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
  --network=pg-network \
  fmcsa_test_image \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=fmcsa_db \
    --table_name=fmcsa_records \
    --url=${URL}
```