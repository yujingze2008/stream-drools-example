package com.gitee.code4fun.storm;

import com.gitee.code4fun.drools.DroolsHelper;
import com.gitee.code4fun.util.JedisUtils;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.StatelessKieSession;

import java.util.Map;

/**
 * @author yujingze
 * @data 2018/8/7
 */
public class DroolsBolt extends BaseBasicBolt {


    @Override
    public void execute(Tuple tuple, BasicOutputCollector basicOutputCollector) {

        String message = String.valueOf(tuple.getValueByField("value"));

        DroolsHelper.getInstance().fireExampleRules(message);

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");
    }
}
