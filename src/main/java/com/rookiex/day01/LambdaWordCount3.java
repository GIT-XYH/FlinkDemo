package com.rookiex.day01;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author RookieX
 * @Date 2021/8/18 4:34 下午
 * @Description:
 */
public class LambdaWordCount3 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8088);

        lines.flatMap((String line, Collector<Tuple2<String, Integer>> out)
        -> Arrays.stream(line.split(" "))
        .forEach(w -> out.collect(Tuple2.of(w, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum("f1")
                .print();

        env.execute("wordCount");
    }
}
