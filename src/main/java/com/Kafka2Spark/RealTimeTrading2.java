package com.Kafka2Spark;


import com.Kafka2Spark.dao.ConfigurationManager;
import com.Kafka2Spark.dao.Constants;
import com.Kafka2Spark.dao.OracleConn;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import org.apache.commons.lang.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;
import com.Kafka2Spark.MysqlConfUtil;

public class RealTimeTrading2 {




    public static void insertDetail(List<Map<String, Object>> list,List<Date> dates,String tablename) throws Exception{

        Map<String,Double> result = new HashMap<>();
        Multimap<String,Object> multimap = ArrayListMultimap.create();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        List<Map<String, Object>> ulist = new ArrayList<Map<String, Object>>();

        //System.out.println(list);
        ulist = list.stream().distinct().collect(Collectors.toList());
        //System.out.println(ulist);

        ulist.forEach(p->{
            result.put(p.get(ConfigurationManager.getProperty(Constants.ROW_KEY)).toString(),Double.parseDouble(p.get("columnValue").toString()));
        });


        System.out.println(result);
        System.out.println(d.format(dates.get(0)));
        Date date = new Date();
        System.out.println(d.format(date));


        Connection conn=null;
        Statement exsql=null;
        conn = OracleConn.getconn();

        try {
            //获得连接
            exsql=conn.createStatement();

            for(String key : result.keySet()){
                double value = result.get(key);
                System.out.println(key+"  "+value);
                String key1 = key.substring(0,8);
                String key2 = key.substring(8);
                String sql = "";

                sql="MERGE INTO realtime_bank_transfer T1\n" +
                        "USING (SELECT '"+d.format(dates.get(0))+"' AS biz_dt, "+value+" AS balance, '"+d.format(date)+"' as load_time,'"+key1+"' as init_time,'"+key2+"' as attr1 FROM dual)  T2\n" +
                        "ON ( T1.Attr1=T2.Attr1)\n" +
                        "WHEN MATCHED THEN\n" +
                        "UPDATE SET T1.biz_dt=T2.biz_dt,T1.balance=T2.balance,T1.load_time=T2.load_time\n" +
                        "WHEN NOT MATCHED THEN INSERT VALUES(T2.biz_dt,T2.balance,T2.load_time,T2.init_time,T2.attr1)";
                System.out.println(sql);

                //返回一个进行此操作的结果，要么成功，要么失败，如果返回的结果>0就是成功，反之失败
                int ora_insert_result=exsql.executeUpdate(sql);
                if(ora_insert_result>0) {
                    System.out.println("realtime_bank_transfer 添加成功");
                }
                else {
                    System.out.println("添加失败");
                }
            }


        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            if (exsql!=null) exsql.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static void insertSum(List<Map<String, Object>> list,List<Date> dates,String tablename) throws Exception{

        Map<String,Double> result = new HashMap<>();
        Multimap<String,Object> multimap = ArrayListMultimap.create();
        SimpleDateFormat d = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");


        list.forEach(p->{
            multimap.put(p.get(ConfigurationManager.getProperty(Constants.ROW_KEY)).toString(),p.get("columnValue"));
        });


        multimap.keySet().forEach(key->{
            Double totalScore = multimap.get(key).stream().map(p->{
                return Double.parseDouble(p.toString());
            }).reduce(0.0,Double::sum);
            result.put(key,totalScore);
        });
        System.out.println(result);
        System.out.println(d.format(dates.get(0)));
        Date date = new Date();
        System.out.println(d.format(date));


        Connection conn=null;
        Statement exsql=null;
        conn = OracleConn.getconn();

        try {
            //获得连接
            exsql=conn.createStatement();

            for(String key : result.keySet()){
                double value = result.get(key);
                System.out.println(key+"  "+value);
                String key1 = key.substring(0,8);
                String key2 = key.substring(8);


                //创建插入的sql语句
                String sql="insert into "+tablename+" (BIZ_DT,BALANCE,LOAD_TIME,INIT_DATE,ATTR1) values('"+d.format(dates.get(0))+"',"+value+",'"+d.format(date)+"',"+"'"+key1+"',"+"'"+key2+"')";
                System.out.println(sql);

                //返回一个进行此操作的结果，要么成功，要么失败，如果返回的结果>0就是成功，反之失败
                int ora_insert_result=exsql.executeUpdate(sql);
                if(ora_insert_result>0) {
                    System.out.println("realtime_trading_amount 添加成功");
                }
                else {
                    System.out.println("添加失败");
                }
            }


        } catch (SQLException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }

        try {
            if (exsql!=null) exsql.close();
            if (conn!=null) conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public static boolean isJson(String content) {
        if(StringUtils.isEmpty(content)){
            return false;
        }
        boolean isJsonObject = true;
        boolean isJsonArray = true;
        try {
            JSONObject.parseObject(content);
        } catch (Exception e) {
            isJsonObject = false;
        }
        try {
            JSONObject.parseArray(content);
        } catch (Exception e) {
            isJsonArray = false;
        }
        if(!isJsonObject && !isJsonArray){ //不是json格式
            return false;
        }
        return true;
    }

    public static void main(String[] args) throws InterruptedException {

        System.setProperty("java.security.auth.login.config", ConfigurationManager.getProperty(Constants.SECURITY_AUTH_LOGIN_CONFIG));
        System.setProperty("java.security.krb5.conf", ConfigurationManager.getProperty(Constants.SECURITY_KRB5_CONFIG));


        // 构建SparkStreaming上下文
        SparkConf conf = new SparkConf().setMaster("local").setAppName("RealTimeTrading");

        //控制sparkstreaming启动时，积压问题并设置背压机制，自适应批次的record变化，来控制任务的堆
        // (1)确保在kill任务时，能够处理完最后一批数据，再关闭程序，不会发生强制kill导致数据处理中断，没处理完的数据丢失
        conf.set("spark.streaming.stopGracefullyOnShutdown", "true");
        //(2)开启后spark自动根据系统负载选择最优消费速率
        conf.set("spark.streaming.backpressure.enabled", "true");
        //(3)开启的情况下，限制第一次批处理应该消费的数据，因为程序冷启动 队列里面有大量积压，防止第一次全部读取，造成系统阻塞
        conf.set("spark.streaming.backpressure.initialRate", "1000");
        //(4)限制每秒每个消费线程读取每个kafka分区最大的数据量
        conf.set("spark.streaming.kafka.maxRatePerPartition", "10000");
        /**
         * 注意：
         只有（4）激活的时候，每次消费的最大数据量，就是设置的数据量，如果不足这个数，就有多少读多少，如果超过这个数字，就读取这个数字的设置的值
         只有（2）+（4）激活的时候，每次消费读取的数量最大会等于（4）设置的值，最小是spark根据系统负载自动推断的值，消费的数据量会在这两个范围之内变化根据系统情况，但第一次启动会有多少读多少数据。此后按（2）+（4）设置规则运行
         （2）+（3）+（4）同时激活的时候，跟上一个消费情况基本一样，但第一次消费会得到限制，因为我们设置第一次消费的频率了
         */

        // 每隔5秒钟，sparkStreaming作业就会收集最近5秒内的数据源接收过来的数据
        JavaStreamingContext jssc = new JavaStreamingContext(conf,Durations.seconds(15));



        //jssc.checkpoint("D:\\streaming_checkpoint");
        String group_id = "realtime_trading_consumer";

        //创建map类型以传参
        Map<String, Object> kafkaParams = new HashMap<String, Object>();
        kafkaParams.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        kafkaParams.put("group.id", group_id);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);

        //kerberos安全认证
        kafkaParams.put("security.protocol", "SASL_PLAINTEXT");
        kafkaParams.put("sasl.mechanism", "GSSAPI");
        kafkaParams.put("sasl.kerberos.service.name", "kafka");


        String topics = ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[] topicArr = topics.split(",");

        Collection<String> topicSet = Arrays.asList(topicArr);
        //HashSet topicSet = new HashSet<String>();
        //topicSet.add(ConfigurationManager.getProperty(Constants.KAFKA_TOPICS));

        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);
        Map<TopicPartition, Long> maptopic = new HashMap<>();

        maptopic = MysqlConfUtil.getOffsetMap(group_id,topicArr);


        try {
            // 获取kafka的数据

            final JavaInputDStream<ConsumerRecord<String, String>> Dstream ;

            if(maptopic.size()<1){
                Dstream = KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams)
                        );
            } else {
                Dstream =
                        KafkaUtils.createDirectStream(
                                jssc,
                                LocationStrategies.PreferConsistent(),
                                ConsumerStrategies.<String, String>Subscribe(topicSet, kafkaParams,maptopic)
                        );
            }

            String[] tablename_rule1 = {"REALTIME","CBPREALTIME"};
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
            String[] operType_rule2 = {"U"};
            //operType  D：delete;I:insert;U:update:DT:truncate;
            String[] trans_type_rule = {"01","02"};//转账类型
            String[] money_type_rule = {"0"};//货币代码
            String[] bktrans_status_rule = {"2"};//转账状态
            String[] business_flag_rule = {"1001"};//开户标志
            String[] asset_prop_rule = {"0"};//账户属性


            //逐一处理每条消息
            Dstream.foreachRDD(rdd-> {
                OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
                System.out.print("-----------------------" + offsetRanges + "-----------------------------------\n");

                /*rdd.foreachPartition(consumerRecords -> {
                    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                    System.out.println(
                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
                });*/

                rdd.foreachPartition(partitions -> {

                    List<Map<String, Object>> list_trading = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> list_opencusts = new ArrayList<Map<String, Object>>();
                    List<Map<String, Object>> list_banktransfer = new ArrayList<Map<String, Object>>();
                    List<Date> dates = new ArrayList<Date>();

                    OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                    System.out.println(
                            o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());


                    partitions.forEachRemaining(line -> {
                        //System.out.print("***************************" + line.value() + "***************************\n");
                        //List<String> list2 = new ArrayList<>();

                        //todo 获取到kafka的每条数据 进行操作
                        //System.out.print("***************************" + s.value() + "***************************");
                        SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        JSONObject res1 = JSON.parseObject(line.value());
                        String newlineg =  line.value().replace("\r|\n", "");
                        //System.out.print("-----------------------" + res1.values() + "-----------------------------------\n");
                        String tablename = res1.getString("Tablename");
                        String timeload = res1.getString("timeload");
                        String operType = res1.getString("operType");

                        try{
                            dates.add(format.parse(timeload));
                        }catch (Exception e){
                            e.printStackTrace();
                        }
                        JSONObject res2 = null;

                        if(Arrays.asList(operType_rule2).contains(operType)){
                             res2 = JSON.parseObject(res1.getString("columnInfo"));
                        } else if(Arrays.asList(operType_rule).contains(operType)){
                             res2 = JSON.parseObject(res1.getString("columns"));
                        }


                        //处理交易额1
                        if (Arrays.asList(tablename_rule1).contains(tablename)
                                &Arrays.asList(operType_rule).contains(operType)) {
                            //operType  D：delete;I:insert;U:update:DT:truncate;
                            //System.out.print("***************************" + res2.values() + "***************************\n");
                            //String asset_prop = res2.getString("ASSET_PROP");
                            String exchange_type = res2.getString("EXCHANGE_TYPE");//{"1","2"};//交易类别
                            String branch_no = res2.getString("BRANCH_NO");//{"8888","9800","9900"};//自营机构
                            String real_status = res2.getString("REAL_STATUS");//real_status_rule = {"0","4"};//处理标志
                            String real_type = res2.getString("REAL_TYPE");//real_type_rule1 = {"0"};//成交类型
                            //String current_balance = res2.getString("CURRENT_BALANCE");
                            //String correct_balance = res2.getString("CORRECT_BALANCE");
                            //String rowkey = UUID.randomUUID().toString();
                            String rowkey = res2.getString("POSITION_STR");
                            if(Arrays.asList(exchange_type_rule).contains(exchange_type)
                                    &!Arrays.asList(branch_no_rule).contains(branch_no)
                                    &Arrays.asList(real_status_rule).contains(real_status)
                                    &Arrays.asList(real_type_rule1).contains(real_type)){

                                Map<String, Object> map1 = new HashMap<String, Object>();
                                map1.put("rowKey", rowkey);
                                map1.put("columnFamily", "columns");
                                map1.put("columnName", res2.getString("INIT_DATE")+res2.getString("STOCK_CODE"));
                                map1.put("columnValue", res2.getString("BUSINESS_BALANCE"));
                                //map1.put("stockType", res2.getString("STOCK_CODE"));
                                list_trading.add(map1);
                            }
                        //处理交易额2
                        }else if (Arrays.asList(tablename_rule2).contains(tablename)
                                &Arrays.asList(operType_rule).contains(operType)){
                            //System.out.print("***************************" + res2.values() + "***************************\n");
                            //String asset_prop = res2.getString("ASSET_PROP");
                            String exchange_type = res2.getString("EXCHANGE_TYPE");
                            String branch_no = res2.getString("BRANCH_NO");
                            String real_status = res2.getString("REAL_STATUS");
                            String real_type = res2.getString("REAL_TYPE");
                            String entrust_bs = res2.getString("ENTRUST_BS");
                            String business_balance = res2.getString("BUSINESS_BALANCE");
                            //String correct_balance = res2.getString("CORRECT_BALANCE");
                            String rowkey = res2.getString("POSITION_STR");
                            if(Arrays.asList(exchange_type_rule).contains(exchange_type)
                                    &!Arrays.asList(branch_no_rule).contains(branch_no)
                                    &Arrays.asList(real_status_rule).contains(real_status)
                                    &Arrays.asList(real_type_rule2).contains(real_type)
                                    &Arrays.asList(entrust_bs_rule).contains(entrust_bs)) {

                                Map<String, Object> map2 = new HashMap<String, Object>();
                                map2.put("rowKey", rowkey);
                                map2.put("columnFamily", "columns");
                                map2.put("columnName", res2.getString("INIT_DATE")+res2.getString("STOCK_CODE"));
                                map2.put("columnValue", res2.getString("BUSINESS_BALANCE"));
                                map2.put("stockType", res2.getString("STOCK_CODE"));
                                list_trading.add(map2);
                            }
                        //处理当日开户数
                        }else if (Arrays.asList(tablename_rule3).contains(tablename)
                                &Arrays.asList(operType_rule).contains(operType)){

                            String business_flag = res2.getString("BUSINESS_FLAG");
                            String asset_prop = res2.getString("ASSET_PROP");
                            //String correct_balance = res2.getString("CORRECT_BALANCE");
                            String rowkey = res2.getString("POSITION_STR");
                            if(Arrays.asList(business_flag_rule).contains(business_flag)
                                    &Arrays.asList(asset_prop_rule).contains(asset_prop)) {

                                Map<String, Object> map3 = new HashMap<String, Object>();
                                map3.put("rowKey", rowkey);
                                map3.put("columnFamily", "columnInfo");
                                map3.put("columnName", res2.getString("INIT_DATE")+res2.getString("BRANCH_NO"));
                                map3.put("columnValue", "1");
                                map3.put("stockType", res2.getString("STOCK_CODE"));
                                list_opencusts.add(map3);
                            }
                        //处理银证转账净转入统计
                        }else if (Arrays.asList(tablename_rule4).contains(tablename)
                                &Arrays.asList(operType_rule2).contains(operType)){

                            String trans_type = res2.getString("TRANS_TYPE");
                            String money_type = res2.getString("MONEY_TYPE");
                            String bktrans_status = res2.getString("BKTRANS_STATUS");
                            String occur_balance = res2.getString("OCCUR_BALANCE");
                            String rowkey = res2.getString("POSITION_STR");
                            Map<String, Object> map4= new HashMap<String, Object>();
                            map4.put("rowkey", operType+rowkey);
                            map4.put("columnFamily", "columns");
                            map4.put("columnName", res2.getString("INIT_DATE")+rowkey);
                            map4.put("moneytype", "0");
                            map4.put("fund_account", "F00001bwl");

                            if(trans_type.equals("02")){
                                occur_balance = "-"+ occur_balance;

                            }
                            //System.out.println("************************ 负数:" +trans_type+"--"+ occur_balance + "**************************");
                            map4.put("columnValue",occur_balance);
                            //map4.put("stockType", res2.getString("STOCK_CODE"));
                            list_banktransfer.add(map4);

                        }else {
                            System.out.println("************************ not match:" + res1.values() + "**************************");
                        }
                    });

                    //调用方法处理数据
                    //TableName hbtable = TableName.valueOf("fundreal");
                    try{
                        //insertMany(hbtable,list);
                        if (list_trading.size()>0) {

                            insertSum(list_trading, dates,"realtime_trading_amount");

                        } else if(list_opencusts.size()>0) {

                            insertSum(list_opencusts, dates,"realtime_open_custs");

                        } else if(list_banktransfer.size()>0) {

                            //insertDetail(list_banktransfer, dates,"realtime_bank_transfer");
                            //EsConfUtil.insertBatch("fundjour",list_banktransfer);

                        } else{
                            System.out.println("没有符合条件的数据");
                        }



                    }catch (Exception e){
                        e.printStackTrace();
                    }

                    MysqlConfUtil.saveOffsetRanges(o,group_id);
                });
               // ((CanCommitOffsets) Dstream.inputDStream()).commitAsync(offsetRanges);

            });

            jssc.start();
            jssc.awaitTermination();
            jssc.close();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}

