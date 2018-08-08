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
        try {
            long begin = System.currentTimeMillis();
            FactType factType = DroolsHelper.getInstance().getFactType("com.myspace.flink_rule", "approve");
            Object applicant = factType.newInstance();
            String[] ss = String.valueOf(tuple.getValueByField("value")).split(",");
            String eventId = ss[0];
            String name = ss[1];
            String score = ss[2];
            factType.set(applicant, "name", name);
            factType.set(applicant, "creditScore", Integer.parseInt(score));
            StatelessKieSession session = DroolsHelper.getInstance().getStatelessSession();
            session.execute(applicant);

            long end = System.currentTimeMillis();
            System.out.println("cast:" + (end - begin) + " ms");

            String result = factType.get(applicant, "name") + "," + factType.get(applicant, "creditScore") + "," + factType.get(applicant, "approved");

            System.out.println(result);

            JedisUtils.set(eventId,result);

        } catch (Exception e) {
            System.out.println("error!!!");
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {

    }

    @Override
    public void prepare(Map stormConf, TopologyContext context) {
        DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");
    }
}
