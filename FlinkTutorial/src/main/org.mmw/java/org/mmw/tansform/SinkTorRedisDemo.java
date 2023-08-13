package org.mmw.tansform;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SinkTorRedisDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<MyData2> sourceStream = env.addSource(new MyDataSource2()); // 得到数据源
        sourceStream.addSink(new RedisSink()); // 核心！保存到redis
        sourceStream.print();
        env.execute("Flink_to_mysql demo");
    }
}
