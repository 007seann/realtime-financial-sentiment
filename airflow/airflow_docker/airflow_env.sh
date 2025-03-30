# # Set Java environment variables
# export JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

# # Set Spark environment variables
# export SPARK_VERSION=3.2.1
# export SPARK_HADOOP_VERSION=3.2
# export SPARK_HOME=/opt/spark-$SPARK_VERSION-bin-hadoop$SPARK_HADOOP_VERSION
# export PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin

# Set Java environment variables
export JAVA_HOME=/opt/java/openjdk-21

# Set Spark environment variables
export SPARK_VERSION=3.5.4
export SPARK_HADOOP_VERSION=3
export SPARK_HOME=/opt/spark-${SPARK_VERSION}-bin-hadoop${SPARK_HADOOP_VERSION}
export PATH=$JAVA_HOME/bin:$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
