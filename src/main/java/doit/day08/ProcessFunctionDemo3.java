package doit.day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Tuple;

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
 */
public class ProcessFunctionDemo3 {

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
                //原来使用的方式是，输入一条，调用一次out.collect输出一条
                //但是现在，先将输入的数据攒起来，满足条件再输出
                lst.add(in);
                long currentTime = System.currentTimeMillis();
                long triggerTime =  currentTime + 10000;
                System.out.println("processElement：输入数据的时间：" + currentTime + " , 触发的时间：" + triggerTime);
                ctx.timerService().registerProcessingTimeTimer(triggerTime);
            }

            //定时器启动时会调用的方法
            @Override
            public void onTimer(long timestamp, OnTimerContext ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                System.out.println("onTimer方法中定时器执行的时间：" + timestamp);

                for (Tuple2<String, Integer> tp : lst) {
                    out.collect(tp);
                }

            }
        }).print();



        env.execute();

    }


}
