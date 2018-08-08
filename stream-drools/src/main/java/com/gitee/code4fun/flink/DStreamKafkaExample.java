package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import com.gitee.code4fun.util.JedisUtils;
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
                String result = "";
                try{
                    long begin = System.currentTimeMillis();
                    String[] ss = s.split(",");
                    String eventId = ss[0];
                    String name = ss[1];
                    String score = ss[2];

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
                    factType.set(applicant, "name", name);
                    factType.set(applicant, "creditScore", Integer.parseInt(score));
                    StatelessKieSession session = DroolsHelper.getInstance().getStatelessSession();
                    session.execute(applicant);

                    long end = System.currentTimeMillis();
                    System.out.println("cast:" + (end - begin) + " ms");

                    result = factType.get(applicant, "name") + "," + factType.get(applicant, "creditScore") + "," + factType.get(applicant, "approved");

                    JedisUtils.set(eventId,result);
                }catch (Exception e){
                    e.printStackTrace();
                }
                return result;
            }
        });

        dataStream.print();

        env.execute();
    }

}
