package doit.day08;

import org.apache.flink.api.common.eventtime.SerializableTimestampAssigner;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.OutputTag;

import java.time.Duration;

/**
 * 使用side output获取窗口迟到的数据
 */
public class GetWindowLateDataDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1000,spark,5
        //2000,spark,7
        //3000,hive,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> tp3DataStream = lines.map(new MapFunction<String, Tuple3<Long, String, Integer>>() {
            @Override
            public Tuple3<Long, String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(Long.parseLong(fields[0]), fields[1], Integer.parseInt(fields[2]));
            }
        }).setParallelism(1);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> streamWithWaterMark = tp3DataStream.assignTimestampsAndWatermarks(WatermarkStrategy.<Tuple3<Long, String, Integer>>forBoundedOutOfOrderness(Duration.ofSeconds(2)).withTimestampAssigner(new SerializableTimestampAssigner<Tuple3<Long, String, Integer>>() {
            @Override
            public long extractTimestamp(Tuple3<Long, String, Integer> tp, long l) {
                //Tuple3（1000,spark,5）
                return tp.f0;
            }
        }));

        KeyedStream<Tuple3<Long, String, Integer>, String> keyedStream = streamWithWaterMark.keyBy(t -> t.f1);



        //在划分窗口
        WindowedStream<Tuple3<Long, String, Integer>, String, TimeWindow> window = keyedStream.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        OutputTag<Tuple3<Long, String, Integer>> lateTag = new OutputTag<Tuple3<Long, String, Integer>>("late-date") {
        };

        //将迟到的数据打上指定的标签
        window.sideOutputLateData(lateTag);

        SingleOutputStreamOperator<Tuple3<Long, String, Integer>> mainStream = window.sum(2);

        mainStream.print(); //输出正常的流（即未打标签的）

        DataStream<Tuple3<Long, String, Integer>> lateStream = mainStream.getSideOutput(lateTag);

        lateStream.print("late-data");

        env.execute();
    }

}
