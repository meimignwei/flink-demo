package org.mmw.sql;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        //1.在创建表的DDL中直接定义时间属性
        String creatDDL = "CREATE TABLE clickTable (" +
                " `user` STRING," +
                " url STRING," +
                " ts BIGINT," +
                " et AS TO_TIMESTAMP( FROM_UNIXTIME(ts / 1000))," + //事件时间  FROM_UNIXTIME() 能转换为年月日时分秒这样的格式 转换秒
                " WATERMARK FOR et AS et - INTERVAL '1' SECOND " + //watermark 延迟一秒
                ") WITH (" +
                " 'connector' = 'filesystem'," +
                " 'path' = '/Users/mingweimei/IdeaProjects/flink-demo/FlinkTutorial/input/clicks.txt'," +
                " 'format' = 'csv'" +
                ")";

        tableEnv.executeSql(creatDDL);
        // top 2
        Table overWindow = tableEnv.sqlQuery("SELECT user, cnt, row_num " +
                "FROM (" +
                " SELECT *, ROW_NUMBER() OVER (" +
                "    ORDER BY cnt DESC" +
                "      ) as row_num " +
                "    FROM (select user,count(url) as cnt FROM clickTable GROUP BY user )" +
                ") WHERE row_num <= 2");
        tableEnv.toChangelogStream(overWindow).print("top 2 is:");
        env.execute();
    }
}
