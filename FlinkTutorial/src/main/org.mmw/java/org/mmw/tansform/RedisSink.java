package org.mmw.tansform;

import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import redis.clients.jedis.Jedis;
import org.apache.flink.configuration.Configuration;
import redis.clients.jedis.JedisPool;

import java.util.HashMap;

public class RedisSink extends RichSinkFunction<MyData2> {
    private Jedis jedis = null;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        String host = "127.0.0.1";
        int port = 6379;
        JedisPool pool = new JedisPool(host, port);
        jedis = pool.getResource();
    }

    @Override
    public void invoke(MyData2 value, Context context) {
        HashMap<String, String> map = new HashMap<>();
        map.put("timestamp", String.valueOf(value.getTimestamp()));
        map.put("num", String.valueOf(value.getNum()));
        map.put("value_list", String.valueOf(value.getValueList()[0]));
        String key = String.valueOf(value.getTimestamp());
        jedis.hmset(key, map); // 保存的格式是key:时间戳，value: 全部的内容
        jedis.expire(key, 120); // 生存周期 5s (5s后在redis数据库中删除)
    }

    @Override
    public void close() throws Exception {
        super.close();
        jedis.close();
        if (jedis != null) {
            jedis.close();
        }
    }
}


