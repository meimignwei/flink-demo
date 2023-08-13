package org.mmw.window;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.util.Collector;

public class ProcessWindowFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

        DataStreamSource<Tuple3<String,String,Integer>> input = env.fromElements(ENGLISH);
        input.keyBy(x -> x.f0)
                .countWindow(1)
                //public abstract class ProcessWindowFunction<IN, OUT, KEY, W extends Window> extends AbstractRichFunction ...
                .process(new ProcessWindowFunction<Tuple3<String, String, Integer>, Tuple3<String,String,Integer>, String, GlobalWindow>() {
                    @Override
                    public void process(String s,   //参数1：key
                                        Context context,    //参数2：上下文对象
                                        Iterable<Tuple3<String, String, Integer>> iterable, //参数3：这个窗口的所有元素
                                        //参数4：收集器，用于向下游传递数据
                                        Collector<Tuple3<String, String, Integer>> collector) throws Exception {
                        System.out.println(context.window().maxTimestamp());
                        int sum = 0 ;
                        String name = "" ;
                        for (Tuple3<String,String,Integer> tuple3:iterable){
                            sum += tuple3.f2 ;
                            name = tuple3.f1 ;
                        }

                        collector.collect(Tuple3.of(s,name,sum));
                    }
                }).print();

        env.execute() ;

    }

    public static final Tuple3[] ENGLISH = new Tuple3[]{
            //班级 姓名 成绩
            Tuple3.of("class1","张三",100),
            Tuple3.of("class1","李四",30),
            Tuple3.of("class1","王五",70),
            Tuple3.of("class2","赵六",50),
            Tuple3.of("class2","小七",40),
            Tuple3.of("class2","小八",10),
    };

}
