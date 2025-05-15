unset HIVE_CONF_DIR HADOOP_OPTS HADOOP_MAPRED_HOME HADOOP_COMMON_HOME HADOOP_CONF_DIR HADOOP_HDFS_HOME HADOOP_COMMON_LIB_NATIVE_DIR HADOOP_YARN_HOME

export JAVA_HOME=/usr/lib/jvm/java-1.8.0/
export HADOOP_HOME=/data/astro/scratch/tallada/shepherd_dev/hadoop
export HIVE_HOME=/data/astro/scratch/tallada/shepherd_dev/hive
export HIVE_AUX_JARS_PATH=/data/astro/scratch/tallada/shepherd_dev/jars
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
export HIVE_OPTS="\
--hiveconf mapred.job.tracker=local \
--hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:/data/astro/scratch/${USER}/shepherd_dev/metastore_db;create=true \
--hiveconf fs.default.name=file:///data/astro/scratch/${USER}/shepherd_dev/dfs \
--hiveconf hive.metastore.warehouse.dir=file:///data/astro/scratch/${USER}/shepherd_dev/dfs/warehouse \
--hiveconf mapreduce.map.memory.mb=2048 \
--hiveconf mapreduce.reduce.memory.mb=2048"
export HADOOP_CLIENT_OPTS="-Xmx8192m"
export HADOOP_CLASSPATH=${HOME}/src/pic-hive-udf/target/classes/

# To create metastore_db
# schematool -initSchema -dbType derby

# hive -hiveconf hive.root.logger=DEBUG,console --debug

# Coverage reports ar available at
# https://jupyter.pic.es/user/tallada/proxy/3000/target/site/jacoco/index.html
