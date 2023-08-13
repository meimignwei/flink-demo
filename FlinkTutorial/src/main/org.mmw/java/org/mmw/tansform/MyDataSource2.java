package org.mmw.tansform;

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

public class MyDataSource2 implements SourceFunction<MyData2> {
    // 定义标志位，用来控制数据的产生
    private boolean isRunning = true;
    private final Random random = new Random(0);

    @Override
    public void run(SourceContext ctx) throws Exception {
        while (isRunning) {
            ctx.collect(new MyData2(random.nextInt(3), System.currentTimeMillis(), 1, new double[]{random.nextDouble()}));
            Thread.sleep(1000L); // 1s生成1个数据
        }
    }

    @Override
    public void cancel() {
        isRunning = false;
    }
}

