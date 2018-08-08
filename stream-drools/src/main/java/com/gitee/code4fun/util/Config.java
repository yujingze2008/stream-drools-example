package com.gitee.code4fun.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * @author yujingze
 * @data 2018/8/8
 */
public class Config {

    protected static Properties properties = new Properties();

    static {
        InputStream resourceAsStream = null;
        try {
            resourceAsStream = Config.class.getResourceAsStream("/config.properties");
            properties.load(resourceAsStream);
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (resourceAsStream != null) {
                try {
                    resourceAsStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    public static final String ZOOKEEPER_HOST = properties.getProperty("zookeeper.host");
    public static final String KAFKA_BOOTSTRAP_SERVERS = properties.getProperty("kafka.bootstrap-servers");
    public static final String REDIS_HOST = properties.getProperty("redis.host");
    public static final int REDIS_PORT = Integer.parseInt(properties.getProperty("redis.port"));
    public static final int REDIS_MAX_TOTAL = Integer.parseInt(properties.getProperty("redis.max-total"));
    public static final int REDIS_MAX_IDLE = Integer.parseInt(properties.getProperty("redis.max-idle"));
    public static final boolean REDIS_TESTONBORROW = Boolean.valueOf(properties.getProperty("redis.testonborrow"));

}
