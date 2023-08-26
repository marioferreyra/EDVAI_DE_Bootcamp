# Remove old Northwind Analytics files in HDFS
/home/hadoop/hadoop/bin/hdfs dfs -rm -r /sqoop/ingest/clientes/*

# Download Clientes from Northwind DB
/usr/lib/sqoop/bin/sqoop import \
--connect jdbc:postgresql://172.17.0.3:5432/northwind \
--username postgres \
--password-file file:///home/hadoop/scripts/sqoop_edvai_postgres.password \
--query "select c.customer_id, c.company_name, sum(od.quantity) as productos_vendidos from orders o inner join order_details od on o.order_id = od.order_id left join customers c on o.customer_id = c.customer_id where \$CONDITIONS group by c.customer_id, c.company_name order by sum(od.quantity) desc" \
--m 1 \
--target-dir /sqoop/ingest/clientes \
--as-parquetfile \
--delete-target-dir
