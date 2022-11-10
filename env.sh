export JAVA_HOME=/data/astro/software/centos7/jvm/jdk8
export HADOOP_HOME=/data/astro/scratch/tallada/apps/hadoop
export HIVE_HOME=/data/astro/scratch/tallada/apps/hive
export HIVE_AUX_JARS_PATH=/data/astro/scratch/tallada/hadoop/jars
export PATH=$HADOOP_HOME/bin:$HIVE_HOME/bin:$PATH
export HIVE_OPTS="\
--hiveconf mapred.job.tracker=local \
--hiveconf javax.jdo.option.ConnectionURL=jdbc:derby:/data/astro/scratch/tallada/hadoop/metastore_db;create=true \
--hiveconf fs.default.name=file:///data/astro/scratch/tallada/hadoop/dfs \
--hiveconf hive.metastore.warehouse.dir=file:///data/astro/scratch/tallada/hadoop/dfs/warehouse \
--hiveconf mapreduce.map.memory.mb=2048 \
--hiveconf mapreduce.reduce.memory.mb=2048"
export HADOOP_CLIENT_OPTS="-Xmx8192m"
export HADOOP_CLASSPATH=/nfs/pic.es/user/t/tallada/src/pic-hadoop-udf/target/classes/

# hive -hiveconf hive.root.logger=DEBUG,console --debug

# Coverage reports ar available at
# https://jupyter.pic.es/user/tallada/proxy/3000/target/site/jacoco/index.html
