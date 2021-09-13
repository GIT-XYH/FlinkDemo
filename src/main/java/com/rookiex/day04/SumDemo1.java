package com.rookiex.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class SumDemo1 {

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

        //如果keyedStream中对应的数据是Tuple，可以使用下标进行聚合
        //keyed.sum(1).print();

        //如果keyedStream中对应的数据是Tuple或自定义的Pojo，可以使用字段名称进行聚合
        keyed.sum("f1").print();

        env.execute();

    }
}
