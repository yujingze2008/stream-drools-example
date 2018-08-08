package com.gitee.code4fun.spark;

import com.gitee.code4fun.drools.DroolsHelper;
import com.gitee.code4fun.util.Config;
import com.google.common.collect.ImmutableMap;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

/**
 * @author yujingze
 * @data 18/7/23
 */
public class SparkStreamingExample {


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingExample");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(500));

        JavaPairReceiverInputDStream messages = KafkaUtils.createStream(jsc, Config.ZOOKEEPER_HOST, "streams4rules",
                ImmutableMap.of("rules_event",3));

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> tp2) throws Exception {
                return tp2._2();
            }
        });


        lines.foreachRDD(rdd -> {
            rdd.foreach(rs -> {
                DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");
                DroolsHelper.getInstance().fireExampleRules(rs);
            });
        });

        jsc.start();
        jsc.awaitTermination();

    }

}
