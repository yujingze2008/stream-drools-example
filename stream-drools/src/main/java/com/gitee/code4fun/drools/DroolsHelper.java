package com.gitee.code4fun.drools;

import com.gitee.code4fun.util.JedisUtils;
import org.drools.compiler.kproject.ReleaseIdImpl;
import org.kie.api.KieBase;
import org.kie.api.KieServices;
import org.kie.api.builder.KieScanner;
import org.kie.api.definition.type.FactType;
import org.kie.api.event.kiescanner.KieScannerEventListener;
import org.kie.api.event.kiescanner.KieScannerStatusChangeEvent;
import org.kie.api.event.kiescanner.KieScannerUpdateResultsEvent;
import org.kie.api.runtime.KieContainer;
import org.kie.api.runtime.KieSession;
import org.kie.api.runtime.StatelessKieSession;

/**
 * @author yujingze
 * @data 2018/8/7
 */
public class DroolsHelper {

    private static volatile DroolsHelper helper;

    protected KieServices ks;

    protected KieContainer container;

    protected KieScanner scanner;

    private DroolsHelper() {
    }

    public static DroolsHelper getInstance() {
        if (helper == null) {
            synchronized (DroolsHelper.class) {
                if (helper == null) {
                    helper = new DroolsHelper();
                }
            }
        }
        return helper;
    }

    public void loadGav(String groupId, String artifactId, String version) {
        ReleaseIdImpl releaseId = new ReleaseIdImpl(groupId, artifactId, version);
        ks = KieServices.Factory.get();
        container = ks.newKieContainer(releaseId);
        scanner = ks.newKieScanner(container);
        scanner.addListener(new KieScannerEventListener() {
            @Override
            public void onKieScannerStatusChangeEvent(KieScannerStatusChangeEvent kieScannerStatusChangeEvent) {
                //System.out.println("*********status:" + kieScannerStatusChangeEvent.getStatus());
            }

            @Override
            public void onKieScannerUpdateResultsEvent(KieScannerUpdateResultsEvent kieScannerUpdateResultsEvent) {
            }
        });
        scanner.start(1000);
    }

    public StatelessKieSession getStatelessSession() {
        return container.newStatelessKieSession();
    }

    public KieSession getKieSession() {
        return container.newKieSession();
    }

    public FactType getFactType(String packageName, String beanName) {
        KieBase kieBase = container.getKieBase();
        FactType factType = kieBase.getFactType(packageName, beanName);
        return factType;
    }

    public String fireExampleRules(String message) {
        String result = null;
        try {
            long begin = System.currentTimeMillis();
            FactType factType = this.getFactType("com.myspace.flink_rule", "approve");
            Object applicant = factType.newInstance();
            String[] values = message.split(",");
            String eventId = values[0];
            String name = values[1];
            String score = values[2];
            factType.set(applicant, "name", name);
            factType.set(applicant, "creditScore", Integer.parseInt(score));
            StatelessKieSession session = this.getStatelessSession();
            session.execute(applicant);
            long end = System.currentTimeMillis();

            System.out.println("cast:" + (end - begin) + " ms");
            result = factType.get(applicant, "name") + "," + factType.get(applicant, "creditScore") + "," + factType.get(applicant, "approved");
            System.out.println(result);

            JedisUtils.set(eventId, result);

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
        return result;
    }

}
