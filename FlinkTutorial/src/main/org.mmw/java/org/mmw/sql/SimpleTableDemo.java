package org.mmw.sql;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.mmw.source.ClickSource;
import org.mmw.tansform.Event;

import java.time.Duration;

import static org.apache.flink.table.api.Expressions.$;

public class SimpleTableDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        SingleOutputStreamOperator<Event> stream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                                .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                                    @Override
                                    public long extractTimestamp(Event element, long recordTimestamp) {
                                        return element.timestamp;
                                    }
                                })
                );
        // 创建表执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        //将datasream转换为table
        Table table = tableEnv.fromDataStream(stream);
        // 直接写sql进行转换
//        Table result = tableEnv.sqlQuery("select user,url from " + table);
//        tableEnv.toDataStream(result).print("result");
        // 基于table直接转换
        Table result2 = table.select($("user"), $("url")).where($("user").isEqual("Alice"));
        tableEnv.toDataStream(result2).print("result");
        // 聚合转换
        tableEnv.createTemporaryView("clickTable",result2);
        // 会对数据流进行更新
        Table table1 = tableEnv.sqlQuery("select user,count(url) as cnt From clickTable group by  user");
        tableEnv.toChangelogStream(table1).print();

        env.execute();
    }
}
