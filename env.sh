export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-amd64
export HADOOP_HOME=~/Projectes/hadoop-3.3.1-src/hadoop-dist/target/hadoop-3.3.1
export HIVE_HOME=~/Projectes/apache-hive-3.1.2-src/packaging/target/apache-hive-3.1.2-bin/apache-hive-3.1.2-bin
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
export HIVE_OPTS="--hiveconf mapred.job.tracker=local --hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:metastore_db;create=true --hiveconf fs.default.name=file:///home/tallada/Projectes/hadoop/dfs --hiveconf hive.metastore.warehouse.dir=file:///home/tallada/Projectes/hadoop/dfs/warehouse"
export HIVE_AUX_JARS_PATH=/home/tallada/Projectes/hadoop/jars
export HADOOP_CLASSPATH=/home/tallada/Projectes/pic-hadoop-udf/target/classes/
