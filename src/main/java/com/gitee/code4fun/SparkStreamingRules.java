package com.gitee.code4fun;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * @author yujingze
 * @data 18/7/23
 */
public class SparkStreamingRules {


    public static void main(String[] args) throws Exception{

        ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "myrule", "LATEST");//LATEST
        KieServices ks = KieServices.Factory.get();
        KieContainer container = ks.newKieContainer(releaseId);
        KieBase base = container.getKieBase();
        /*KieScanner scanner = ks.newKieScanner(container);
        scanner.start(1000);*/
        //StatelessKieSession session = container.newStatelessKieSession();

        SparkConf conf = new SparkConf();
        conf.setAppName("SparkStreamingRules");
        JavaStreamingContext jsc = new JavaStreamingContext(conf,new Duration(3000));

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
            rdd.foreach(rs -> {
                JSONObject json = JSON.parseObject(rs);
                String name = json.getString("name");
                int creditScore = json.getInteger("creditScore");
                FactType factType = base.getFactType("com.myspace.myrule", "Approve");
                Object applicant = factType.newInstance();
                factType.set(applicant, "name", name);
                factType.set(applicant, "creditScore", creditScore);
                StatelessKieSession session = container.newStatelessKieSession();
                session.execute(applicant);
                System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));
            });
        });


        jsc.start();
        jsc.awaitTermination();

    }

}
