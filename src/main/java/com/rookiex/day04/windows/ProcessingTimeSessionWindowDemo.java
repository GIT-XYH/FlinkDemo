package com.rookiex.day04.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先KeyBy按照ProcessingTime划分会话窗口
 * Keyed Window（并行度可以是多个）
 */
public class ProcessingTimeSessionWindowDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //spark,5
        //spark,7
        //hive,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        //先keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);
        //在划分窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyed.window(ProcessingTimeSessionWindows.withGap(Time.seconds(10)));

        window.sum(1).print();

        env.execute();
    }

}
