<img src="https://i.ibb.co/5RM26Cw/LOGO-COLOR2.png" width="500px">

Hadoop Commands
===============

**Listar archivos**

```
hdfs dfs -ls /
```

**Remover archivos**

```
hdfs dfs -rm -f /ingest/*
```

**Mover archivos al FileSystem**

```
hdfs dfs -put /home/hadoop/landing/* /ingest
```

**Crear carpeta en el FileSytem**

```
hdfs dfs -mkdir /nifi
```

**Crear carpeta persisos a carpeta**

```
hdfs dfs -chmod 777 /nifi
```
