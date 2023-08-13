package org.mmw.process;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.mmw.source.ClickSource;
import org.mmw.tansform.Event;

import java.sql.Timestamp;

public class KeyedProcessFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .process(new KeyedProcessFunction<String, Event, Object>() {
                    @Override
                    public void processElement(Event event, KeyedProcessFunction<String, Event, Object>.Context context, Collector<Object> collector) throws Exception {
                        long l = context.timerService().currentProcessingTime();
                        collector.collect(context.getCurrentKey() + "数据到达，到达时间戳：" + new Timestamp(l) + "watermark :" + context.timerService().currentWatermark());
                        context.timerService().registerProcessingTimeTimer(l + 10 * 1000L);
                    }

                    @Override
                    public void onTimer(long timestamp, KeyedProcessFunction<String, Event, Object>.OnTimerContext context, Collector<Object> out) {
                        out.collect(context.getCurrentKey() + "定时器触发，触发时间" + new Timestamp(timestamp));
                    }
                }).print();
        env.execute();
    }
}
