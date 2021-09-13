package com.rookiex.day04.windows;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 按照条数划分窗口
 *
 * 按照有没有分区的DataStream划分窗口
 *  Keyed Window，先KeyBy再划分窗口（第一种用的最多）
 *  NonKeyed Window 没有keyBy就划分窗口（几乎很少用）
 *
 *  Keyed Window：Window和Window Function对应的DataStream是多个并行的
 */
//先KeyBy再划分窗口
public class CountWindowDemo {

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
        WindowedStream<Tuple2<String, Integer>, String, GlobalWindow> window = keyed.countWindow(5);
        //对窗口中的数据进行运算（调用WindowFunction）
        SingleOutputStreamOperator<Tuple2<String, Integer>> sum = window.sum(1);
        sum.print();

        env.execute();
    }

}
