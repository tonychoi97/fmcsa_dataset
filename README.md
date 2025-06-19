URL="https://ai.fmcsa.dot.gov/SMS/files/FMCSA_CENSUS1_2025Jun.zip"

docker run -it \
  --network=pg-network \
  fmcsa_test:003 \
    --user=root \
    --password=root \
    --host=pg-database \
    --port=5432 \
    --db=fmcsa_db \
    --table_name=fmcsa_records \
    --url=${URL}