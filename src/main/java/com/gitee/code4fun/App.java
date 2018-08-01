package com.gitee.code4fun;


import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.definition.type.FactType;
import org.kie.api.event.kiescanner.KieScannerEventListener;
import org.kie.api.event.kiescanner.KieScannerStatusChangeEvent;
import org.kie.api.event.kiescanner.KieScannerUpdateResultsEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.StatelessKieSession;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) throws Exception {
        //SNAPSHOT
        ReleaseIdImpl releaseId = new ReleaseIdImpl("com.myspace", "myrule", "LATEST");//LATEST
        KieServices ks = KieServices.Factory.get();
        KieContainer container = ks.newKieContainer(releaseId);
        KieScanner scanner = ks.newKieScanner(container);
        scanner.start(1000);
        StatelessKieSession session = container.newStatelessKieSession();



       // for (int i = 0; i < 10; i++) {

            while (true){
                FactType factType = factType(container.getKieBase());
                //approve applicant = new approve();
                //applicant.setName("张三");
                //applicant.setCreditScore(500);
                Object applicant = makeApplicant(factType);
                session.execute(applicant);
                System.out.println("申请人：" + factType.get(applicant, "name") + "，评分：" + factType.get(applicant, "creditScore") + ",是否可以申请批准" + factType.get(applicant, "approved"));
                //System.out.println("申请人：" + applicant.getName() + "，评分：" + applicant.getCreditScore() + ",是否可以申请批准" + applicant.getApproved());
                Thread.sleep(3000);
            }

        //Thread.sleep(2000);//休眠20秒，等待更新规则查看输出结果
        //}
    }

    private static Object makeApplicant(FactType factType) throws Exception {
        Object applicant = factType.newInstance();
        factType.set(applicant, "name", "张三");
        factType.set(applicant, "creditScore", 200);
        return applicant;
    }

    protected static FactType factType(KieBase base) {
        FactType factType = base.getFactType("com.myspace.myrule", "Approve");
        return factType;
    }
}

