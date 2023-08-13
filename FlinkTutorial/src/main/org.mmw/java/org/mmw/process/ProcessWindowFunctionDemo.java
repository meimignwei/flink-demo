package org.mmw.process;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.mmw.source.ClickSource;
import org.mmw.tansform.Event;

public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        DataStreamSource<Event> stream = env.addSource(new ClickSource());
        stream.keyBy(data -> data.user)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .process(new ProcessWindowFunction<Event, Object, String, TimeWindow>() {
                    @Override
                    public void process(String s, ProcessWindowFunction<Event, Object, String, TimeWindow>.Context context, Iterable<Event> iterable, Collector<Object> collector) throws Exception {
                        int sum = 0;
                        for(Event i : iterable){
                            sum += 1;
                        }
                        collector.collect(context.window().getStart() +" " + s + " " + sum + " " + context.window().getEnd());
                    }
                }).print();
        env.execute();
    }

}
