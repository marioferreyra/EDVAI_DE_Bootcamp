# Remove old tripdata files
rm -rf /home/hadoop/landing/results.csv
rm -rf /home/hadoop/landing/drivers.csv
rm -rf /home/hadoop/landing/constructors.csv
rm -rf /home/hadoop/landing/races.csv

# Download F1 file into /home/hadoop/landing
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/results.csv
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/drivers.csv
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/constructors.csv
wget -P /home/hadoop/landing https://data-engineer-edvai.s3.amazonaws.com/f1/races.csv

# Remove old tripdata files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/results.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/drivers.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/constructors.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/races.csv

# Insert tripdata files into HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/results.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/drivers.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/constructors.csv /ingest
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/races.csv /ingest
