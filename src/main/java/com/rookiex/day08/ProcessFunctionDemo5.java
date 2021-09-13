package com.rookiex.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

/**
 * ProcessFunction 是Flink流式计算底层API，可以访问底层的属性和方法
 * <p>
 * 1.可以处理数
 * 2.可以使用状态（只针对于KeyedStream）
 * 3.可以使用定时器（只针对于KeyedStream）
 * <p>
 * KeyedStream使用ProcessFunction并使用Timer（定时器）
 *
 * 使用ProcessingTime + KeyedProcessFunction+Timer实现类似滚动窗口的功能
 *
 */
public class ProcessFunctionDemo5 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //spark,1
        //spark,5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> tpStream = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f0);

        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private List<Tuple2<String, Integer>> lst = new ArrayList<>();

            @Override
            public void processElement(Tuple2<String, Integer> in, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {
                long currentTime = System.currentTimeMillis();
                long triggerTime =currentTime - (currentTime % 10000) + 10000;
                System.out.println("processElement：输入数据的时间：" + currentTime + " , 触发的时间：" + triggerTime);
                lst.add(in);
                //如果注册了多个时间相同的定时器，后面的会将前面注册的定时器覆盖，只执行一个
                ctx.timerService().registerProcessingTimeTimer(triggerTime);

            }

            //定时器启动时会调用的方法
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                System.out.println("onTimer方法中定时器执行的时间：" + timestamp);
                for (Tuple2<String, Integer> tp : lst) {
                    out.collect(tp);
                }
                lst.clear();

            }
        }).print();



        env.execute();

    }


}
