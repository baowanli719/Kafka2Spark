package com.Kafka2Spark;

import com.Kafka2Spark.dao.ConfigurationManager;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Arrays;
import java.util.Properties;

import com.Kafka2Spark.dao.Constants;

public class KafkaTest {
    public static void main(String[] args){

        System.setProperty("java.security.auth.login.config", "D:\\myconf\\jaas.conf");
        System.setProperty("java.security.krb5.conf", "D:\\myconf\\krb5.conf");

        Properties properties = new Properties();
        //properties.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));

        properties.put("bootstrap.servers", ConfigurationManager.getProperty(Constants.KAFKA_BOOTSTRAP_SERVERS));
        properties.put("group.id", "group-1");
        properties.put("enable.auto.commit", "true");
        properties.put("auto.commit.interval.ms", "1000");
        properties.put("auto.offset.reset", "earliest");
        properties.put("session.timeout.ms", "30000");
        properties.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        //kerberos安全认证
        properties.put("security.protocol", "SASL_PLAINTEXT");
        properties.put("sasl.mechanism", "GSSAPI");
        properties.put("sasl.kerberos.service.name", "kafka");

        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);
        kafkaConsumer.subscribe(Arrays.asList( "real-banktransfer"));
        while (true) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(100);
            //System.out.println("here");
            for (ConsumerRecord<String, String> record : records) {
                System.out.printf("offset = %d, value = %s", record.offset(), record.value());
                System.out.println();
            }
        }

    }
}