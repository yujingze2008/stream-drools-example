package com.gitee.code4fun;

import com.myspace.mytest.approve;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author yujingze
 * @data 2018/8/6
 */
public class DStreamExample {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();

        DataStream<String> ds = env.socketTextStream("localhost", 9999);


        ds.print();

        ds = ds.map(new MapFunction<String, String>() {

            @Override
            public String map(String s) throws Exception {

                long begin = System.currentTimeMillis();

                String[] ss = s.split(",");

                //SNAPSHOT
                ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "flink_rule", "LATEST");//LATEST
                KieServices ks = KieServices.Factory.get();
                KieContainer container = ks.newKieContainer(releaseId);
                //KieScanner scanner = ks.newKieScanner(container);
                //scanner.start(1000);

                /*StatelessKieSession session = container.newStatelessKieSession();

                FactType factType = container.getKieBase().getFactType("com.myspace.flink_rule", "approve");
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
                session.execute(applicant);
*/


                //return factType.get(applicant, "name") + "," + factType.get(applicant, "creditScore") + "," + factType.get(applicant, "approved");

                KieSession kieSession = container.newKieSession();

                approve bean = new approve();
                bean.setName(ss[0]);
                bean.setCreditScore(Integer.parseInt(ss[1]));

                kieSession.insert(bean);
                int x = kieSession.fireAllRules();

                kieSession.dispose();

                System.out.println(x);

                long end = System.currentTimeMillis();

                System.out.println("cast:"+(end-begin)+" ms");

                return bean.getName() + "," + bean.getCreditScore() + "," + bean.getApproved();


            }
        });

        ds.print();

        env.execute();

    }

}
