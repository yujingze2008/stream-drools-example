package com.gitee.code4fun.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author yujingze
 * @data 2018/8/8
 */
public class KafkaUtils {

    protected static Producer<String, String> producer = null;

    static {
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        producer = new KafkaProducer(properties);
    }

    public static void sendMessage(ProducerRecord record){
        producer.send(record);
        producer.flush();
    }

    public static void sendMessage(String topic,String message){
        sendMessage(new ProducerRecord(topic,message));
    }

}
