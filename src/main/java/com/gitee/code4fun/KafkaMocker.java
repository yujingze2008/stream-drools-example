package com.gitee.code4fun;


import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Arrays;
import java.util.Properties;
import java.util.Random;

/**
 * Unit test for simple App.
 */
public class KafkaMocker
{
    /**
     * Rigorous Test :-)
     */
    public void send() throws Exception
    {


        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093,localhost:9094");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.IntegerSerializer");
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer");

        //properties.setProperty(ProducerConfig.ACKS_CONFIG,"-1");

        Producer<String,String> producer = new KafkaProducer(properties);

        for(int i=0;i<1000;i++){
            JSONObject json = new JSONObject();
            json.put("name","zhangsan");
            json.put("creditScore",new Random().nextInt(1000));
            ProducerRecord record = new ProducerRecord("rules_event",json.toString());
            producer.send(record);
        }

        producer.flush();

    }

    public void receive() throws Exception{
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        props.put("group.id", "test");
        //props.put("enable.auto.commit", "true");
        //props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Arrays.asList("rules_event"));
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
            }
        }
    }

    public static void main(String[] args) throws Exception{
        KafkaMocker mocker = new KafkaMocker();
        mocker.send();
        //mocker.receive();
    }

}
