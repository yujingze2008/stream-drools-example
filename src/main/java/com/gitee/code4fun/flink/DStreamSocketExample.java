package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import com.gitee.code4fun.drools.entitys.Approve;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.kie.api.runtime.KieSession;

/**
 * @author yujingze
 * @data 2018/8/6
 */
public class DStreamSocketExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 9999);

        ds.print();

        ds = ds.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {

                long begin = System.currentTimeMillis();

                String[] ss = s.split(",");
                DroolsHelper.getInstance().loadGav("com.myspace", "flink_rule", "LATEST");
                KieSession kieSession = DroolsHelper.getInstance().getKieSession();
                Approve bean = new Approve();
                bean.setName(ss[0]);
                bean.setCreditScore(Integer.parseInt(ss[1]));
                kieSession.insert(bean);
                kieSession.dispose();

                long end = System.currentTimeMillis();

                System.out.println("cast:" + (end - begin) + " ms");

                return bean.getName() + "," + bean.getCreditScore() + "," + bean.getApproved();

            }
        });

        ds.print();

        env.execute();

    }

}
