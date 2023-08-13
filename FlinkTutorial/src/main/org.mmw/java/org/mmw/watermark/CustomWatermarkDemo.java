package org.mmw.watermark;

import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.mmw.source.ClickSource;
import org.mmw.tansform.Event;

// 自定义水位线的产生
public class CustomWatermarkDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.addSource(new ClickSource())
                .assignTimestampsAndWatermarks(new CustomWatermarkStrategy())
                .print();
        env.execute();
    }
    //内部静态类
    public static class CustomWatermarkStrategy implements WatermarkStrategy<Event> {
        @Override
        //createTimestampAssigner方法生成TimeStampAssigner
        public TimestampAssigner<Event> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
            return new SerializableTimestampAssigner<Event>() {
                @Override
                //extractTimestamp,目的是从当前数据提取时间戳
                public long extractTimestamp(Event element, long recordTimestamp)
                {
                    return element.timestamp; // 告诉程序数据源里的时间戳是哪一个字段
                }
            };
        }
        @Override
        //createWatermarkGenerator生成WatermarkGenerator
        public WatermarkGenerator<Event> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
            return new CustomPeriodicGenerator();
        }
    }
    //CustomPeriodicGenerator实现WatermarkGenerator接口，并重写方法
    public static class CustomPeriodicGenerator implements WatermarkGenerator<Event> {
        private Long delayTime = 5000L; // 延迟时间
        private Long maxTs = Long.MIN_VALUE + delayTime + 1L; // 观察到的最大时间戳
        @Override
        //更新当前时间戳，这边不发送水位线，目的是保存时间戳
        public void onEvent(Event event, long eventTimestamp, WatermarkOutput
                output) {
            // 每来一条数据就调用一次
            maxTs = Math.max(event.timestamp, maxTs); // 更新最大时间戳
        }
        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 发射水位线，默认 200ms 调用一次
            //-1毫秒都是为了贴切窗口闭合的时候左闭右开设计
            output.emitWatermark(new Watermark(maxTs - delayTime - 1L));
        }
    }
}


