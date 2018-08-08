package com.gitee.code4fun.spark;

import com.gitee.code4fun.drools.DroolsHelper;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.StatelessKieSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yujingze
 * @data 18/7/23
 */
public class SparkStreamingExample {


    public static void main(String[] args) throws Exception {

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingExample");
        JavaStreamingContext jsc = new JavaStreamingContext(conf, new Duration(3000));

        Map<String, Integer> topicmap = new HashMap<String, Integer>();
        topicmap.put("rules_event", 3);
        String zk = "localhost:2181,localhost:2182,localhost:2183";
        JavaPairReceiverInputDStream messages = KafkaUtils.createStream(jsc, zk, "streams4rules", topicmap);

        JavaDStream<String> lines = messages.map(new Function<Tuple2<String, String>, String>() {

            @Override
            public String call(Tuple2<String, String> tp2) throws Exception {
                return tp2._2();
            }
        });


        lines.foreachRDD(rdd -> {
            rdd.foreach(rs -> {
                long begin = System.currentTimeMillis();
                String[] ss = rs.split(",");
                DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");

                FactType factType = DroolsHelper.getInstance().getFactType("com.myspace.flink_rule", "approve");
                Object applicant = null;
                try {
                    applicant = factType.newInstance();
                } catch (InstantiationException e) {
                    e.printStackTrace();
                } catch (IllegalAccessException e) {
                    e.printStackTrace();
                }
                factType.set(applicant, "name", ss[0]);
                factType.set(applicant, "creditScore", Integer.parseInt(ss[1]));
                StatelessKieSession session = DroolsHelper.getInstance().getStatelessSession();
                session.execute(applicant);

                long end = System.currentTimeMillis();
                System.out.println("cast:" + (end - begin) + " ms");

                System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));

            });
        });


        jsc.start();
        jsc.awaitTermination();

    }

}
