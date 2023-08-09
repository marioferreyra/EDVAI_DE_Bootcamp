rm -f /home/hadoop/landing/*

wget -P /home/hadoop/landing https://github.com/fpineyro/homework-0/blob/master/starwars.csv

/home/hadoop/hadoop/bin/hdfs dfs -rm -f /ingest/*

/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/landing/* /ingest
