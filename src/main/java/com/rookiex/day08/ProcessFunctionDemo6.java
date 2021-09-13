package com.rookiex.day08;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * ProcessFunction 是Flink流式计算底层API，可以访问底层的属性和方法
 * <p>
 * 1.可以处理数
 * 2.可以使用状态（只针对于KeyedStream）
 * 3.可以使用定时器（只针对于KeyedStream）
 * <p>
 * KeyedStream使用ProcessFunction并使用Timer（定时器）
 *
 * 使用EventTime + KeyedProcessFunction+Timer实现类似滚动窗口的功能
 *
 */
public class ProcessFunctionDemo6 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //1000,spark,1
        //2000,spark,5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //streamWithWaterMark
        SingleOutputStreamOperator<String> streamWithWaterMark = lines.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((line, time) -> Long.parseLong(line.split(",")[0])));

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tpStream = streamWithWaterMark.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        });

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = tpStream.keyBy(t -> t.f1);

        keyedStream.process(new KeyedProcessFunction<String, Tuple3<Long, String, Integer>, Tuple3<Long, String, Integer>>() {

            private transient ListState<Tuple3<Long, String, Integer>> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Tuple3<Long, String, Integer>> stateDescriptor = new ListStateDescriptor<>("lst-state", TypeInformation.of(new TypeHint<Tuple3<Long, String, Integer>>() {
                }));
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<Long, String, Integer> in, Context ctx, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                //ctx.timerService().currentWatermark()
                Long eventTime = in.f0;
                long triggerTime =eventTime- (eventTime % 10000) + 10000;
                listState.add(in);
                ctx.timerService().registerEventTimeTimer(triggerTime);
            }

            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
                System.out.println("onTimer invoked, 触发的时间为：" + timestamp);
                for (Tuple3<Long, String, Integer> tp : listState.get()) {
                    out.collect(tp);
                }
                listState.clear();
            }
        }).print();



        env.execute();

    }


}
