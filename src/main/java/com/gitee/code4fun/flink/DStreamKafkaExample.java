package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.StatelessKieSession;

import java.util.Properties;

/**
 * @author yujingze
 * @data 2018/8/7
 */
public class DStreamKafkaExample {

    public static void main(String[] args) throws Exception {

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094");
        properties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>("rules_event", new SimpleStringSchema(), properties);

        DataStream<String> dataStream = env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                long begin = System.currentTimeMillis();
                String[] ss = s.split(",");
                DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");

                /*KieSession kieSession = DroolsHelper.getInstance().getKieSession();
                Approve bean = new Approve();
                bean.setName(ss[0]);
                bean.setCreditScore(Integer.parseInt(ss[1]));
                kieSession.insert(bean);
                kieSession.fireAllRules();
                kieSession.dispose();*/

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
                return factType.get(applicant, "name") + "," + factType.get(applicant, "creditScore") + "," + factType.get(applicant, "approved");
            }
        });

        dataStream.print();

        env.execute();
    }

}