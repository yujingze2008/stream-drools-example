package com.gitee.code4fun;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * @author yujingze
 * @data 18/7/23
 */
public class KafkaStreaming {


    public static void main(String[] args) throws Exception{
        Logger.getLogger("org").setLevel(Level.ERROR);

        SparkConf conf = new SparkConf();
        conf.setAppName("streams4rules");

        JavaStreamingContext jsc = new JavaStreamingContext(conf,new Duration(3000));

        /*Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        Collection<String> topics = Arrays.asList("rules_event");

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe(topics, kafkaParams)
                );*/


        Map<String,Integer> topicmap = new HashMap<String,Integer>();
        topicmap.put("rules_event",3);
        String zk = "localhost:2181,localhost:2182,localhost:2183";
        JavaPairReceiverInputDStream messages = KafkaUtils.createStream(jsc,zk,"streams4rules",topicmap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String,String>,String>() {

            @Override
            public String call(Tuple2<String, String> tp2) throws Exception {
                return tp2._2();
            }
        });


        lines.foreachRDD(rdd -> {
            rdd.foreach(rs -> System.out.println("!!!!!!!!!"+rs));
        });


        //lines.print();

        jsc.start();
        jsc.awaitTermination();

    }

}
