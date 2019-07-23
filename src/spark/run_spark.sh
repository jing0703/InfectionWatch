#!/bin/bash

nohup /usr/local/spark/bin/spark-submit \
	--packages com.databricks:spark-redshift_2.11:2.0.1 \
	--jars /usr/local/spark/jars/RedshiftJDBC41-1.1.17.1017.jar,spark-csv_2.11-1.5.0.jar,spark-avro_2.11-4.0.0.jar \
	/home/ubuntu/InfectionWatch/src/spark/transformation.py


nohup /usr/local/spark/bin/spark-submit \
	--packages com.databricks:spark-redshift_2.11:2.0.1 \
	--jars /usr/local/spark/jars/RedshiftJDBC41-1.1.17.1017.jar,spark-csv_2.11-1.5.0.jar,spark-avro_2.11-4.0.0.jar \
	/home/ubuntu/InfectionWatch/src/spark/annotation.py