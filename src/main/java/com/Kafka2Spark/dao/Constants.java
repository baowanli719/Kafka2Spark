package com.Kafka2Spark.dao;

/**
 * @ClassName: Constants
 * @author: zhl
 * @date: 2019/11/18  13:18
 *  TODO:常量接口
 */
public interface  Constants {
    String GROUP_ID = "group.id";
    String KAFKA_TOPICS = "kafka.topics";
    String KAFKA_TOPICS2 = "kafka.topics2";
    String KAFKA_TOPICS3 = "kafka.topics3";
    String KAFKA_TOPICS4 = "kafka.topics4";
    String KAFKA_TOPICS5 = "kafka.topics5";
    String KAFKA_TOPICS6 = "kafka.topics6";
    String KAFKA_BOOTSTRAP_SERVERS = "bootstrap.servers";
    String TIME_INTERVAL= "time.interval";
    String ZK_SERVERS = "zk.servers";
    String STREAMING_CHECKPOINT_DIR = "streaming.checkpoint.dir";
    String SECURITY_AUTH_LOGIN_CONFIG= "java.security.auth.login.config";
    String SECURITY_KRB5_CONFIG = "java.security.krb5.conf";
    String HBASE_ZOOKEEPER_QUORUM= "hbase.zookeeper.quorum";
    String HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT= "hhbase.zookeeper.property.clientPort";
    String HBASE_MASTER_PRINCIPAL= "hbase.master.principal";
    String HBASE_MASTER_KEYTAB_FILE= "hbase.master.keytab.file";
    String HBASE_REGIONSERVER_PRINCIPAL= "hbase.regionserver.principal";
    String HBASE_REGIONSERVER_KEYTAB_FILE= "hbase.regionserver.keytab.file";
    String HADOOP_SECURITY_AUTHENTICATION= "hadoop.security.authentication";
    String HBASE_SECURITY_AUTHENTICATION= "hbase.security.authentication";
    String CORE_SITE_PATH= "core-site.path";
    String HDFS_SITE_PATH= "hdfs-site.path";
    String HBASE_SITE_PATH= "hbase-site.path";
    String ORA_URL = "ora_url";
    String ORA_USR = "ora_usr";
    String ORA_PWD = "ora_pwd";
    String ROW_KEY = "row_key";
    String ES_CLUSTER_NAME = "es.cluster.name";
    String ES_CLUSTER_SERVER = "es.cluster.server";
    String ES_CLUSTER_PORT= "es.cluster.port";
    String SPARK_MASTER= "spark.master.url";
    String MYSQL_URL = "mysql_url";
    String MYSQL_USR = "mysql_usr";
    String MYSQL_PWD = "mysql_pwd";


}

