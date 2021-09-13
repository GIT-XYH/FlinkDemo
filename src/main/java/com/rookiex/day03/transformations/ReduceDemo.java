package com.rookiex.day03.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class ReduceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //spark
        //hadoop
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String s) throws Exception {
                return Tuple2.of(s, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> reduced = keyed.reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> tp1, Tuple2<String, Integer> tp2) throws Exception {
                tp1.f1 = tp1.f1 + tp2.f1;
                return tp1;
            }
        });

        reduced.print();

        env.execute();

    }
}
