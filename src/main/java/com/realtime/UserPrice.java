package com.realtime;


import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.Date;

public class UserPrice {


    
    public static void main(String[] args) throws InterruptedException {

        System.setProperty("java.security.auth.login.config", ConfigurationManager.getProperty(Constants.SECURITY_AUTH_LOGIN_CONFIG));
        System.setProperty("java.security.krb5.conf", ConfigurationManager.getProperty(Constants.SECURITY_KRB5_CONFIG));

        Long seconds = Long.parseLong(ConfigurationManager.getProperty(Constants.TIME_INTERVAL));
        String myMaster = MyCManager.getProperty(MyConstants.SPARK_MASTER);
        // 构建SparkStreaming上下
        //SparkConf conf = new SparkConf().setAppName("UserPrice2Hbase");//.setMaster(myMaster);
        SparkConf conf = new SparkConf().setAppName("UserPrice2Hbase").setMaster(myMaster);

        // 每隔5秒钟，sparkStreaming作业就会收集5秒内的数据源接收过来的数
        JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.seconds(seconds));

        //jssc.checkpoint("D:\\streaming_checkpoint");

        //创建map类型以传
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put("group.id", "realtime_userprice2hbase_consumer");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //kerberos安全认证
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");


        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS2);
        String[] topicArr = topics.split(",");

        Collection<String> topicSet = Arrays.asList(topicArr);
        //HashSet topicSet = new HashSet<String>();
        //topicSet.add(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS));
        kafkaParams.put("auto.offset.reset", "latest");
        //kafkaParams.put("auto.offset.reset", "earliest");
        kafkaParams.put("enable.auto.commit", false);
        Map<TopicPartition, Long> maptopic = new HashMap<>();


        try {
            // 获取kafka的数
            final JavaInputDStream<ConsumerRecord<String, String>> Dstream =
                    KafkaUtils.createDirectStream(
                            jssc,
                            LocationStrategies.PreferConsistent(),
                            ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                    );

            String[] tablename_rule1 = {"PRICE"};
            String[] tablename_rule2 = {"CRDTREALTIME",""};
            String[] tablename_rule3 = {"FUNDACCOUNTJOUR"};
            String[] tablename_rule4 = {"BANKTRANSFER"};
            String[] entrust_bs_rule = {"1","2"};//委托种类
            String[] exchange_type_rule = {"1","2"};//交易类别
            String[] branch_no_rule = {"8888","9800","9900"};//自营机构
            String[] real_status_rule = {"0","4"};//处理标志
            String[] real_type_rule1 = {"0"};//成交类型
            String[] real_type_rule2 = {"6","7","8","9"};//成交类型
            String[] stock_type_rule = {"0","1","d","c","h","e","g","D","L","6","T","p","q"};//证券类别
            String[] operType_rule = {"I"};
            String[] operType_rule2 = {"I","U"};
            //operType  D：delete;I:insert;U:update:DT:truncate;
            String[] trans_type_rule = {"01","02"};//转账类型
            String[] money_type_rule = {"0","1"};//货币代码
            String[] bktrans_status_rule = {"2"};//转账状
            String[] asset_prop_rule = {"0"};//账户属
            String[] business_flag_rule = {"2041","2042","2141","2142"};//业务品种
            

            //逐一处理每条消息
            Dstream.foreachRDD(rdd-> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                //System.out.print("-----------------------" + offsetRanges + "-----------------------------------\n");

                rdd.foreachPartition(partitions -> {

                    List<Map<String, Object>> list_userprice = new ArrayList<Map<String, Object>>();
                    List<Date> dates = new ArrayList<Date>();

                    partitions.forEachRemaining(line -> {
                        //System.out.print("***************************" + line.value() + "***************************\n");
                        //List<String> list2 = new ArrayList<>();
                        //todo 获取到kafka的每条数 进行操作
                        //System.out.print("***************************" + line.value() + "***************************");
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        String newline = line.value().replace("\n|\r","");
                        boolean flag = false;
                        flag=MyUtils.isJson(newline);
                        JSONObject res1 = null;
                        String tablename = null;
                        String timeload = null;
                        String operType = null;
                        
                        if(flag){
                        	res1 = JSON.parseObject(newline);
                            tablename = res1.getString("Tablename");
                            timeload = res1.getString("timeload");
                            operType = res1.getString("operType");
                            try{
                                dates.add(format.parse(timeload));
                            }catch (Exception e){
                                e.printStackTrace();
                            }
                            
	                        JSONObject res2 = null;
	                        
		                    if(res1.containsKey("columnInfo")){
		                        res2 = JSON.parseObject(res1.getString("columnInfo"));
		                    } else if(res1.containsKey("columns")){
		                        res2 = JSON.parseObject(res1.getString("columns"));
		                    } else {
		                        flag = false;
		                    }
	
	
	                        if (Arrays.asList(tablename_rule1).contains(tablename)
	                                &Arrays.asList(operType_rule2).contains(operType)
	                                &flag){
	
	                            String stock_code = res2.getString("STOCK_CODE");
	                            String exchange_type = res2.getString("EXCHANGE_TYPE");
	                            String money_type = res2.getString("MONEY_TYPE");
	                            String asset_price = res2.getString("ASSET_PRICE");
	                            String rowkey = stock_code+"_"+exchange_type;
	                            SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	                            Date date = new Date();
	                            String storagetime = d.format(date);
	                            
	                            Map<String, Object> map1= new HashMap<String, Object>();
                                map1.put("rowKey", rowkey);
                                map1.put("columnFamily", "info");
                                map1.put("columnName", "stock_code");
                                map1.put("columnValue",stock_code);
                                list_userprice.add(map1);
                                
                                Map<String, Object> map2= new HashMap<String, Object>();
                                map2.put("rowKey", rowkey);
                                map2.put("columnFamily", "info");
                                map2.put("columnName", "exchange_type");
                                map2.put("columnValue",exchange_type);
                                list_userprice.add(map2);
                                
                                Map<String, Object> map3= new HashMap<String, Object>();
                                map3.put("rowKey", rowkey);
                                map3.put("columnFamily", "info");
                                map3.put("columnName", "money_type");
                                map3.put("columnValue",money_type);
                                list_userprice.add(map3);
                                
                                Map<String, Object> map4= new HashMap<String, Object>();
                                map4.put("rowKey", rowkey);
                                map4.put("columnFamily", "info");
                                map4.put("columnName", "asset_price");
                                map4.put("columnValue",asset_price);
                                list_userprice.add(map4);
                                
                                
                                Map<String, Object> map5= new HashMap<String, Object>();
                                map5.put("rowKey", rowkey);
                                map5.put("columnFamily", "info");
                                map5.put("columnName", "timeload");
                                map5.put("columnValue",timeload);
                                list_userprice.add(map5);
                                
                                Map<String, Object> map6= new HashMap<String, Object>();
                                map6.put("rowKey", rowkey);
                                map6.put("columnFamily", "info");
                                map6.put("columnName", "storagetime");
                                map6.put("columnValue",storagetime);
                                list_userprice.add(map6);
                                
                                
	
	                        }else {
	                            //System.out.println("************************ not match:" + res1.values() + "**************************");
	                        }
                    	}
                    });

                    try{
                        //insertMany(hbtable,list);
                        if(list_userprice.size()>0) {

                            //insertDetail(list_fundjour, dates,"realtime_bank_transfer");
                        	MyUtils.insertDetail2Hbase(list_userprice,"RL_ARM:HS_USER_PRICE");
                        	
                        	//MyUtils.insertDetail2Oracle(list_fundjour, dates,"realtime_bank_transfer");

                        } else{
                            System.out.println("没有符合条件的数");
                        }
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

