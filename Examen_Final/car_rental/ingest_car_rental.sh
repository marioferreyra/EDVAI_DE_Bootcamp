# Remove old Car Rental files
rm -rf /home/hadoop/car_rental/CarRentalData.csv
rm -rf /home/hadoop/car_rental/CarRentalData02.csv

# Download Car Rental file into /home/hadoop/civil_aviation
wget -P /home/hadoop/car_rental https://data-engineer-edvai.s3.amazonaws.com/CarRentalData.csv
wget -P /home/hadoop/car_rental -O /home/hadoop/car_rental/CarRentalData02.csv https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/georef-united-states-of-america-state/exports/csv?lang=en&timezone=America%2FArgentina%2FBuenos_Aires&use_labels=true&delimiter=%3B

# Remove old Car Rental files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /car_rental/CarRentalData.csv
/home/hadoop/hadoop/bin/hdfs dfs -rm -f /car_rental/CarRentalData02.csv

# Insert Car Rental files into HDFS
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/car_rental/CarRentalData.csv /car_rental
/home/hadoop/hadoop/bin/hdfs dfs -put /home/hadoop/car_rental/CarRentalData02.csv /car_rental
