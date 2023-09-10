# Remove old Civil Aviation files
rm -rf /home/hadoop/civil_aviation/2021-informe-ministerio.csv
rm -rf /home/hadoop/civil_aviation/202206-informe-ministerio.csv
rm -rf /home/hadoop/civil_aviation/aeropuertos_detalle.csv

# Download Civil Aviation file into /home/hadoop/civil_aviation
wget -P /home/hadoop/civil_aviation https://data-engineer-edvai.s3.amazonaws.com/2021-informe-ministerio.csv
wget -P /home/hadoop/civil_aviation https://data-engineer-edvai.s3.amazonaws.com/202206-informe-ministerio.csv
wget -P /home/hadoop/civil_aviation https://data-engineer-edvai.s3.amazonaws.com/aeropuertos_detalle.csv

# Remove old Civil Aviation files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /civil_aviation/2021-informe-ministerio.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /civil_aviation/202206-informe-ministerio.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /civil_aviation/aeropuertos_detalle.csv

# Insert Civil Aviation files into HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/civil_aviation/2021-informe-ministerio.csv /civil_aviation
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/civil_aviation/202206-informe-ministerio.csv /civil_aviation
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/civil_aviation/aeropuertos_detalle.csv /civil_aviation
