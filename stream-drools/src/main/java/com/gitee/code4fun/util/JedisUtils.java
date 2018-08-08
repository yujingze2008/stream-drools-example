package com.gitee.code4fun.util;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

/**
 * @author yujingze
 * @data 2018/8/8
 */
public class JedisUtils {

    private static JedisPool jedisPool = null;

    private JedisUtils() {

    }

    static {
        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(Config.REDIS_MAX_TOTAL);
        config.setMaxIdle(Config.REDIS_MAX_IDLE);
        config.setMaxWaitMillis(1000L);
        config.setTestOnBorrow(Config.REDIS_TESTONBORROW);
        jedisPool = new JedisPool(config, Config.REDIS_HOST, Config.REDIS_PORT);
    }

    public static void set(String key, String value) {
        Jedis jedis = jedisPool.getResource();
        try {
            jedis.set(key, value);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            release(jedis);
        }
    }

    public static String get(String key) {
        String value = null;
        Jedis jedis = jedisPool.getResource();
        try {
            value = jedis.get(key);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            release(jedis);
        }
        return value;
    }

    public static void release(Jedis jedis) {
        if (jedis != null) {
            jedisPool.returnResource(jedis);
        }
    }

}
