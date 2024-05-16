https://hadoop.apache.org/docs/stable/hadoop-aws/tools/hadoop-aws/index.html
https://hub.docker.com/r/bitnami/spark/
https://stackoverflow.com/questions/44411493/java-lang-noclassdeffounderror-org-apache-hadoop-fs-storagestatistics/44500698#44500698


spark-submit --master spark://spark-master:7077 --name arrow-spark --deploy-mode client /opt/bitnami/spark/jobs/transform_eod.py


future improvement
incremental update

read data from iceberg