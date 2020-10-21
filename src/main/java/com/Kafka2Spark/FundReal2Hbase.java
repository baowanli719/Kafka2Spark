package com.Kafka2Spark;


import com.Kafka2Spark.dao.ConfigurationManager;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.Kafka2Spark.dao.Constants;

import java.util.*;
import java.util.UUID;


public class FundReal2Hbase {

    /**
     * 声明静态配置
     */
    static Configuration conf = null;
    static Connection conn = null;
    static {
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", ConfigurationManager.getProperty(Constants.ZK_SERVERS));
        //设置连接参数：HBase数据库使用的端口
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        try{
            conn = ConnectionFactory.createConnection(conf);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * 添加数据（多个rowKey，多个列族，适合由固定结构的数据）
     * @param tableName
     * @param list
     * @throws Exception
     */
    public static void insertMany(TableName tableName, List<Map<String, Object>> list) throws Exception{
        List<Put> puts = new ArrayList<Put>();
        //Table负责跟记录相关的操作如增删改查等
        Table table = conn.getTable(tableName);

        if (list != null && list.size() > 0){
            for (Map<String, Object> map: list){
                Put put = new Put(Bytes.toBytes(map.get("rowKey").toString()));
                put.addColumn(Bytes.toBytes(map.get("columnFamily").toString()), Bytes.toBytes(map.get("columnName").toString()), Bytes.toBytes(map.get("columnValue").toString()));
                puts.add(put);
            }
        }
        table.put(puts);
        table.close();
        System.out.println("fund_real,add data Success!");
    }


    public static void main(String[] args) throws InterruptedException {

        System.setProperty("java.security.auth.login.config", "D:\\myconf\\jaas.conf");
        System.setProperty("java.security.krb5.conf", "D:\\myconf\\krb5.conf");

        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setAppName("FUNDREAL").setMaster("local[*]");

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(15));

        //jssc.checkpoint("D:\\streaming_checkpoint");

        //创建map类型以传参
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put("group.id", "test-consumer");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //kerberos安全认证
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");

        HashSet topicSet = new HashSet<String>();
        topicSet.add(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS));

        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        try {
            // 获取kafka的数据
            final JavaInputDStream<ConsumerRecord<String, String>> Dstream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                    );
            String[] tablename_rule = {"FUNDREAL"};//委托种类
            String[] entrust_bs_rule = {"1","2"};//委托种类
            String[] exchange_type_rule = {"1","2"};//交易类别
            String[] branch_no_rule = {"8888","9800","9900"};//自营机构
            String[] real_status_rule = {"0","4"};//处理标志
            String[] real_type_rule = {"6","7","8","9"};//成交类型
            String[] stock_type_rule = {"0","1","d","c","h","e","g","D","L","6","T"};//证券类别
            double s1 = 0;

            //获取words
            //JavaDStream<String> words = stream.flatMap(s -> Arrays.asList(s.value().split(",")).iterator());
            Dstream.foreachRDD(rdd-> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                System.out.print("-----------------------" + offsetRanges + "-----------------------------------\n");

                rdd.foreachPartition(partitions -> {

                    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();


                    partitions.forEachRemaining(line -> {
                        //System.out.print("***************************" + line.value() + "***************************\n");
                        //List<String> list2 = new ArrayList<>();
                        //todo 获取到kafka的每条数据 进行操作
                        //System.out.print("***************************" + s.value() + "***************************");

                        JSONObject res1 = JSON.parseObject(line.value());
                        //System.out.print("-----------------------" + res1.values() + "-----------------------------------\n");

                        String tablename = res1.getString("Tablename");

                        JSONObject res2 = JSON.parseObject(res1.getString("columns"));
                        String asset_prop = res2.getString("ASSET_PROP");
                        //String exchange_type = res2.getString("EXCHANGE_TYPE");
                        String branch_no = res2.getString("BRANCH_NO");
                        String current_balance = res2.getString("CURRENT_BALANCE");
                        String correct_balance = res2.getString("CORRECT_BALANCE");
                        String rowkey = UUID.randomUUID().toString();
                        //operType  D：delete;I:insert;U:update:DT:truncate;

                        if (Arrays.asList(tablename_rule).contains(tablename)) {
                            System.out.print("***************************" + res2.values() + "***************************\n");
                            //System.out.print("-----------------------" + res2.getString("CURRENT_BALANCE") + "-----------------------------------\n");
                            Map<String, Object> map1 = new HashMap<String, Object>();
                            map1.put("rowKey", rowkey);
                            map1.put("columnFamily", "columns");
                            map1.put("columnName", "asset_prop");
                            map1.put("columnValue", res2.getString("ASSET_PROP"));
                            list.add(map1);

                            Map<String, Object> map2 = new HashMap<String, Object>();
                            map2.put("rowKey", rowkey);
                            map2.put("columnFamily", "columns");
                            map2.put("columnName", "branch_no");
                            map2.put("columnValue", res2.getString("BRANCH_NO"));
                            list.add(map2);

                            Map<String, Object> map3 = new HashMap<String, Object>();
                            map3.put("rowKey", rowkey);
                            map3.put("columnFamily", "columns");
                            map3.put("columnName", "current_balance");
                            map3.put("columnValue", res2.getString("CURRENT_BALANCE"));
                            list.add(map3);

                            Map<String, Object> map4 = new HashMap<String, Object>();
                            map4.put("rowKey", rowkey);
                            map4.put("columnFamily", "columns");
                            map4.put("columnName", "correct_balance");
                            map4.put("columnValue", res2.getString("CORRECT_BALANCE"));
                            list.add(map4);

                            Map<String, Object> map5 = new HashMap<String, Object>();
                            map5.put("rowKey", rowkey);
                            map5.put("columnFamily", "columns");
                            map5.put("columnName", "timeload");
                            map5.put("columnValue", res1.getString("timeload"));
                            list.add(map5);

                        }else {
                            System.out.print("*************************** not match **************************\n");
                        }
                    });
                    //调用方法处理数据
                    TableName hbtable = TableName.valueOf("fundreal");
                    try{
                        insertMany(hbtable,list);
                    }catch (Exception e){
                        e.printStackTrace();
                    }
                });
                ((CanCommitOffsets) Dstream.inputDStream()).commitAsync(offsetRanges);
            });

            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
