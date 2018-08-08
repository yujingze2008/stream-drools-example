package com.gitee.code4fun.storm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.kafka.spout.KafkaSpout;
import org.apache.storm.kafka.spout.KafkaSpoutConfig;
import org.apache.storm.topology.TopologyBuilder;

/**
 * @author yujingze
 * @data 2018/8/3
 */
public class StormExample {

    public static void main(String[] args) throws Exception {

        TopologyBuilder builder = new TopologyBuilder();
        KafkaSpoutConfig config = KafkaSpoutConfig.builder(com.gitee.code4fun.util.Config.KAFKA_BOOTSTRAP_SERVERS, "rules_event")
                .setGroupId("storm-kafka-group").build();
        KafkaSpout<String, String> kafkaSpout = new KafkaSpout<>(config);
        builder.setSpout("kafkaSpout", kafkaSpout);
        builder.setBolt("droolsBolt", new DroolsBolt()).localOrShuffleGrouping("kafkaSpout");
        Config stromConfig = new Config();
        stromConfig.setDebug(true);
        stromConfig.setNumWorkers(2);

        LocalCluster localCluster = new LocalCluster();
        localCluster.submitTopology("kafkaSpout",stromConfig,builder.createTopology());

    }

}
