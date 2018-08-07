package com.gitee.code4fun.flink;

import com.myspace.mytest.approve;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieServices;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;

import java.util.Properties;

/**
 * @author yujingze
 * @data 2018/8/7
 */
public class FlinkKafka {

    public static void main(String[] args) throws Exception{

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","localhost:9092,localhost:9093,localhost:9094");
        properties.setProperty("group.id","flink-group");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>("rules_event",new SimpleStringSchema(),properties);
        DataStream<String> dataStream = env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                long begin = System.currentTimeMillis();

                String[] ss = s.split(",");

                //SNAPSHOT
                ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "flink_rule", "LATEST");//LATEST
                KieServices ks = KieServices.Factory.get();
                KieContainer container = ks.newKieContainer(releaseId);

                KieSession kieSession = container.newKieSession();

                approve bean = new approve();
                bean.setName(ss[0]);
                bean.setCreditScore(Integer.parseInt(ss[1]));

                kieSession.insert(bean);
                kieSession.fireAllRules();
                kieSession.dispose();

                long end = System.currentTimeMillis();

                System.out.println("cast:"+(end-begin)+" ms");

                return bean.getName() + "," + bean.getCreditScore() + "," + bean.getApproved();
            }
        });


        dataStream.print();

        env.execute();


    }

}
