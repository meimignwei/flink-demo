package org.mmw.window;

import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class ReduceFunctionOnWindowDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment() ;

        DataStreamSource<Tuple3<String,String,Integer>> input = env.fromElements(ENGLISH);
        input.keyBy(x -> x.f0)
                .countWindow(2)
                .reduce(new ReduceFunction<Tuple3<String, String, Integer>>() {
                    @Override
                    public Tuple3<String, String, Integer> reduce(Tuple3<String, String, Integer> value1, Tuple3<String, String, Integer> value2) throws Exception {

                        System.out.println("value1-->"+value1);
                        System.out.println("value2-->"+value2);

                        System.out.println("==========================");
                        return new Tuple3<>(value1.f0,value1.f1,value1.f2+value2.f2);
                    }
                }).print("reduce累加");

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
