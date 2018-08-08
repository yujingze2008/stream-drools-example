package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import com.gitee.code4fun.util.Config;
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
        properties.setProperty("bootstrap.servers", Config.KAFKA_BOOTSTRAP_SERVERS);
        properties.setProperty("group.id", "flink-group");

        FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<String>("rules_event", new SimpleStringSchema(), properties);


        DataStream<String> dataStream = env.addSource(consumer).map(new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {

                DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");
                String result = DroolsHelper.getInstance().fireExampleRules(s);

                return result;
            }
        });


        dataStream.print();

        env.execute();
    }

}
