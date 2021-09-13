package doit.day05;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.time.Duration;

/**
 * 先KeyBy按照EventTime划分滑动窗口
 * Keyed Window（并行度可以是多个）
 *
 * 使用新的API提取EventTime生成WaterMark
 */
public class EventTimeSlidingWindowNewAPIDemo {

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
        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> streamWithWaterMark = tp3DataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(0)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, Integer>>() {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> tp, long l) {
                //Tuple3（1000,spark,5）
                return tp.f0;
            }
        }));

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = streamWithWaterMark.keyBy(t -> t.f1);


        //在划分窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(SlidingEventTimeWindows.of(Time.seconds(10), Time.seconds(5)));

        window.sum(2).print();

        env.execute();
    }

}
