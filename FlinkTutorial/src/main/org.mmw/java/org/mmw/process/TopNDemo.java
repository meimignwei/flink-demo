package org.mmw.process;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.mmw.source.ClickSource;
import org.mmw.source.UrlViewCount;
import org.mmw.tansform.Event;
import org.mmw.window.UvCountexample;

import java.sql.Timestamp;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/*
* 案例需求：实时统计一段时间内的出现次数最多的水位。
* 例如，统计最近10秒钟内出现次数最多的两个水位，并且每5秒钟更新一次。
* 我们知道，这可以用一个滑动窗口来实现。于是就需要开滑动窗口收集传感器的数据，
* 按照不同的水位进行统计，而后汇总排序并最终输出前两名。这其实就是著名的“Top N”问题。
 */

public class TopNDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.getConfig().setAutoWatermarkInterval(100);
        SingleOutputStreamOperator<Event> datastream = env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(WatermarkStrategy.<Event>forBoundedOutOfOrderness(Duration.ZERO)
                        .withTimestampAssigner(new SerializableTimestampAssigner<Event>() {
                            @Override
                            public long extractTimestamp(Event event, long l) {
                                return event.timestamp;
                            }
                        })
                );
        datastream.map(event -> event.url)
                .windowAll(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)))
                .aggregate(new UrlHashMapCountAgg(), new UrlAllWindowResult())
                .print();


        // 对于同一窗口统计出的访问量，进行收集和排序
        env.execute();
    }

    public static class UrlHashMapCountAgg implements AggregateFunction<String, HashMap<String, Integer>, ArrayList<Tuple2<String, Integer>>>{

        @Override
        public HashMap<String, Integer> createAccumulator() {
            return new HashMap<>();
        }

        @Override
        public HashMap<String, Integer> add(String s, HashMap<String, Integer> stringIntegerHashMap) {
            if (stringIntegerHashMap.containsKey(s)) {
                stringIntegerHashMap.put(s, stringIntegerHashMap.get(s) + 1);
            } else {
                stringIntegerHashMap.put(s, 1);
            }
            return stringIntegerHashMap;
        }

        @Override
        public ArrayList<Tuple2<String, Integer>> getResult(HashMap<String, Integer> stringIntegerHashMap) {
            ArrayList<Tuple2<String, Integer>> result = new ArrayList<>();
            List<Map.Entry<String, Integer>> collect = stringIntegerHashMap.entrySet().stream()
                    .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                    .limit(2)
                    .collect(Collectors.toList());
            for (Map.Entry<String, Integer> enrty:collect) {
                result.add(Tuple2.of(enrty.getKey(), enrty.getValue()));
            }
            return result;
        }

        @Override
        public HashMap<String, Integer> merge(HashMap<String, Integer> stringIntegerHashMap, HashMap<String, Integer> acc1) {
            return null;
        }
    }

    public static class UrlAllWindowResult extends ProcessAllWindowFunction<ArrayList<Tuple2<String, Integer>>, String, TimeWindow> {

        @Override
        public void process(ProcessAllWindowFunction<ArrayList<Tuple2<String, Integer>>, String, TimeWindow>.Context context, Iterable<ArrayList<Tuple2<String, Integer>>> iterable, Collector<String> collector) throws Exception {
            ArrayList<Tuple2<String, Integer>> next = iterable.iterator().next();
            StringBuilder result = new StringBuilder();
            result.append("----------------------");
            result.append("窗口结束时间" + new Timestamp(context.window().getEnd()) + "\n");
            //取list前两个进行使用
            for (int i = 0; i < 2; i++) {
                Tuple2<String, Integer> stringIntegerTuple2 = next.get(i);
                String info = "No" + (i + 1) + " "
                        + "url:" + stringIntegerTuple2.f0 + " "
                        + "访问量:" + stringIntegerTuple2.f1 + " " + "\n";
                result.append(info);
            }
            result.append("------------------------");
            collector.collect(result.toString());
        }
    }
}
