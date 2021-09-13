package doit.day05;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 对已经生成的Window进行操作的Function叫WindowFunction
 * 在窗口中的数据不进行增量聚合，而是将数据攒起来，当窗口触发后在进行计算
 */
public class EventTimeTumblingWindowFunctionDemo3 {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //窗口的长度为5秒
        //[0, 4999], [5000, 9999], [10000, 14999]
        //1000,spark,5
        //2000,spark,7
        //3000,hive,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //tp3DataStream并行度为2，这个流中有水位线吗？（没有）
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tp3DataStream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        }).setParallelism(1);

        //对多个并行的DataStream提取EventTime生成WaterMark
        //有WaterMark的DataStream并行度是多少？   2  ，因为tp3DataStream并行度为2
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> streamWithWaterMark = tp3DataStream
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0))
                        .withTimestampAssigner((tp, l) -> tp.f0)
                );

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = streamWithWaterMark.keyBy(t -> t.f1);


        //在划分窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        //对窗口调用apply方法，就是将窗口内的数据攒起来，然后窗口触发后再进行运算
        window.apply(new WindowFunction<Tuple3<Long, String, Integer>, Tuple2<String, Integer>, String, TimeWindow>() {

            /**
             *
             * @param key     KeyBy的条件
             * @param window  Window对象，可以获取窗口的一些信息
             * @param input   缓存在窗口中的数据（WindowState中，是集合）
             * @param out     使用collector输出数据
             * @throws Exception
             */
            @Override
            public void apply(String key, TimeWindow window, Iterable<Tuple3<Long, String, Integer>> input, Collector<Tuple2<String, Integer>> out) throws Exception {
//                System.out.println("appy方法被调用了！！！！");
//                for (Tuple3<Long, String, Integer> tp : input) {
//                    out.collect(Tuple2.of(tp.f1, tp.f2));
//                }

                int count = 0;
                for (Tuple3<Long, String, Integer> tp : input) {
                    count += tp.f2;
                }
                out.collect(Tuple2.of(key, count));
            }
        }).print();


        env.execute();
    }

}
