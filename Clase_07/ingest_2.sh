# Remove old tripdata files
rm -rf /home/hadoop/landing/yellow_tripdata_2021-01.parquet
rm -rf /home/hadoop/landing/yellow_tripdata_2021-02.parquet

# Download tripdata file into /home/hadoop/landing
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/yellow_tripdata_2021-01.parquet
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/yellow_tripdata_2021-02.parquet

# Remove old tripdata files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/yellow_tripdata_2021-01.parquet
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/yellow_tripdata_2021-02.parquet

# Insert tripdata files into HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-02.parquet /ingest
