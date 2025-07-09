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
  --network=fmcsa-extract-records_default \
  fmcsa_test_image \
    --user=root \
    --password=root \
    --host=pgdatabase \
    --port=5432 \
    --db=fmcsa_db \
    --table_name=fmcsa_records \
    --url=${URL}
```

Note: If the network above isn't working, check what the default network is in .zsh.

```
docker network ls
```

This will bring up a list of Docker networks. If your file is named 'fmcsa-extract-records', the network might be called 'fmcsa-extract-records_default'. Pull the default network into your Docker run command.