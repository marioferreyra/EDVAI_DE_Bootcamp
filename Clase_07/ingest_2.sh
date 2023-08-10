wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/yellow_tripdata_2021-01.parquet
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/yellow_tripdata_2021-02.parquet

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-01.parquet /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/yellow_tripdata_2021-02.parquet /ingest
