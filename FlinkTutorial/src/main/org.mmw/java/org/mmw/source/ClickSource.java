package org.mmw.source;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.mmw.tansform.Event;

import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * @author mmw
 * @date
 *
 * 自定义Source, 用来模拟生成点击数据
 */
public class ClickSource implements SourceFunction<Event> {

    private static Boolean isRunning = true ;

    /**
     * 用于生成数据， 并通过SourceContext将数据发射到数据流中。
     * @param ctx The context to emit elements to and for accessing locks.
     * @throws Exception
     */
    @Override
    public void run(SourceContext<Event> ctx) throws Exception {
        String [] users = {"Zhangsan" , "Lisi" , "Tom" , "Jerry" , "Peiqi","Alice"} ;
        String [] urls = {"/home" , "/detail" , "/cart" ,"/pay"} ;
        Random random = new Random();
        //通过循环持续产生数据
        while(isRunning){
            //构造Event数据
            Event event = new Event(
                    users[random.nextInt(users.length)],
                    urls[random.nextInt(urls.length)],
                    System.currentTimeMillis()
            );
            //通过ctx 将数据发射出去
            ctx.collect(event);
            //发射数据和时间出去
            //ctx.collectWithTimestamp(event,event.getTs());
            //发射水位线
            //ctx.emitWatermark(new Watermark(event.getTs()));//todo 跟在数据后面，不能放到数据前
            //限制每1秒钟生成1条数据
            TimeUnit.SECONDS.sleep(1);
        }
    }

    @Override
    public void cancel() {
        isRunning = false ;
    }
}