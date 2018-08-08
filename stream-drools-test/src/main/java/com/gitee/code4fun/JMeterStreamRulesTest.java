package com.gitee.code4fun;

import com.gitee.code4fun.util.JedisUtils;
import com.gitee.code4fun.util.KafkaUtils;
import org.apache.jmeter.protocol.java.sampler.AbstractJavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;

import java.util.Random;
import java.util.UUID;

/**
 * @author yujingze
 * @data 2018/8/8
 */
public class JMeterStreamRulesTest extends AbstractJavaSamplerClient {


    @Override
    public SampleResult runTest(JavaSamplerContext javaSamplerContext) {

        SampleResult sr = new SampleResult();
        sr.setSampleLabel("stream-drools-sample");
        try {
            sr.sampleStart();
            String eventId = UUID.randomUUID().toString().replaceAll("-", "");
            StringBuffer message = new StringBuffer();
            message.append(eventId)
                    .append(",")
                    .append("zhangsan,")
                    .append(new Random().nextInt(1000));
            KafkaUtils.sendMessage("rules_event", message.toString());
            String result = null;
            while (true) {
                result = JedisUtils.get(eventId);
                if (result != null && !"".equals(result)) {
                    break;
                }
            }
            sr.setResponseData(result);
            sr.setDataType(SampleResult.TEXT);
            sr.setSuccessful(true);
        } catch (Exception e) {
            sr.setSuccessful(false);
        } finally {
            sr.sampleEnd();
        }

        return sr;
    }
}
