package com.gitee.code4fun;


import com.myspace.mytest.approve;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieRepository;
import org.kie.api.builder.KieScanner;
import org.kie.api.definition.type.FactType;
import org.kie.api.event.kiescanner.KieScannerEventListener;
import org.kie.api.event.kiescanner.KieScannerStatusChangeEvent;
import org.kie.api.event.kiescanner.KieScannerUpdateResultsEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

import java.util.Random;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        //SNAPSHOT
        ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "flink_rule", "LATEST");//LATEST
        KieServices ks = KieServices.Factory.get();
        KieContainer container = ks.newKieContainer(releaseId);
        KieScanner scanner = ks.newKieScanner(container);

        scanner.addListener(new KieScannerEventListener() {
            @Override
            public void onKieScannerStatusChangeEvent(KieScannerStatusChangeEvent kieScannerStatusChangeEvent) {
                System.out.println("*********status:"+kieScannerStatusChangeEvent.getStatus());
            }

            @Override
            public void onKieScannerUpdateResultsEvent(KieScannerUpdateResultsEvent kieScannerUpdateResultsEvent) {
                System.out.println("##############Update");
            }
        });

        scanner.start(1000);
        StatelessKieSession session = container.newStatelessKieSession();



       // for (int i = 0; i < 10; i++) {

            while (true){
                FactType factType = factType(container.getKieBase());
                //approve applicant = new approve();
                //applicant.setName("张三");
                //applicant.setCreditScore(999);
                Object applicant = makeApplicant(factType);
                session.execute(applicant);
                System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));
                //System.out.println("申请人：" + applicant.getName() + "，评分：" + applicant.getCreditScore() + ",是否可以申请批准" + applicant.getApproved());

                //session.execute(new Object());

                //System.out.println(1111);

                Thread.sleep(3000);
            }

        //Thread.sleep(2000);//休眠20秒，等待更新规则查看输出结果
        //}
    }

    private static Object makeApplicant(FactType factType) throws Exception {
        Object applicant = factType.newInstance();
        factType.set(applicant, "name", "张三");
        factType.set(applicant, "creditScore", new Random().nextInt(1000));
        return applicant;
    }

    protected static FactType factType(KieBase base) {
        FactType factType = base.getFactType("com.myspace.flink_rule", "approve");
        return factType;
    }
}

