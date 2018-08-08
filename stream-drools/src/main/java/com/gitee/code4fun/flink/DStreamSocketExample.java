package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.StatelessKieSession;

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

        ds.print();

        env.execute();

    }

}
