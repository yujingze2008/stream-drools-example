package com.gitee.code4fun.flink;

import com.gitee.code4fun.drools.DroolsHelper;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.kie.api.definition.type.FactType;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author yujingze
 * @data 2018/8/3
 */
public class DSetBatchExample {

    public static void main(String[] args) throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.createLocalEnvironment();

        DataSet<String> data = env.readTextFile("/Users/yujingze/develop/flink-examples-data/rules-data.txt");

        DataSet<Tuple2<String, Integer>> rulesData = data.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                String[] ss = s.split(",");
                return new Tuple2<String, Integer>(ss[0], Integer.parseInt(ss[1]));
            }
        });

        rulesData.print();

        rulesData.collect().forEach(tuple -> {
            FactType factType = DroolsHelper.getInstance().getFactType("com.myspace.mytest", "approve");
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
            StatelessKieSession session = DroolsHelper.getInstance().getStatelessSession();
            session.execute(applicant);

            System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));
        });

    }

}
