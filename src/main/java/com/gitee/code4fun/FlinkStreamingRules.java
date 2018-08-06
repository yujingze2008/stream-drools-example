package com.gitee.code4fun;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author yujingze
 * @data 2018/8/3
 */
public class FlinkStreamingRules {

    public static void main(String[] args) throws Exception {

        //SNAPSHOT
        ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "myrule", "LATEST");//LATEST
        KieServices ks = KieServices.Factory.get();
        KieContainer container = ks.newKieContainer(releaseId);
        KieScanner scanner = ks.newKieScanner(container);
        scanner.start(1000);
        StatelessKieSession session = container.newStatelessKieSession();

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile("/Users/yujingze/develop/flink-examples-data/rules-data.txt");


        /*data = data.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.equals("World");
            }
        });*/

        data.print();

        DataSet<Tuple2<String,Integer>> newData = data.map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2<String,Integer> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple2<String, Integer>(ss[0],Integer.parseInt(ss[1]));
            }
        });

        newData.print();

        newData.collect().forEach(tuple -> {
            FactType factType = container.getKieBase().getFactType("com.myspace.myrule", "Approve");
            Object applicant = null;
            try {
                applicant = factType.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            factType.set(applicant, "name", tuple.f0);
            factType.set(applicant, "creditScore", tuple.f1);
            session.execute(applicant);
            System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));


        });




        /*DataSet<Tuple2<String, Integer>> counts = text.flatMap((s, collector) -> {
            Arrays.asList(s.split(" ")).forEach(word -> collector.collect(new Tuple2<>(word, 1)));
        });*/


        /*DataSet<Tuple2<String,Integer>> counts = text.flatMap(new FlatMapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public void flatMap(String s, Collector<Tuple2<String,Integer>> collector) throws Exception {
                Arrays.asList(s.split(" ")).forEach(word -> {
                    collector.collect(new Tuple2<String,Integer>(word,1));
                });
            }
        }).groupBy(0).sum(1);

        counts.print();*/

    }

}
