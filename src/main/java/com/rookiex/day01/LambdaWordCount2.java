package com.rookiex.day01;


import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;

/**
 * @Author RookieX
 * @Date 2021/8/18 1:55 下午
 * @Description:
 */
public class LambdaWordCount2 {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> dataStreamSource = executionEnvironment.socketTextStream(args[0], Integer.parseInt(args[1]));

        //将数据压平和 1 合并在一起

        //如果输入类型和输出类型不一致, 使用 lambda 表达式就要使用 return 指定类型
        //如果输入一行, 使用 collector 输出多行, 使用 lambda 表达式就要使用 return 指定类型
        dataStreamSource.flatMap((String line, Collector<Tuple2<String, Integer>> out)
                -> Arrays.stream(line.split(" "))
                .forEach(w -> out.collect(Tuple2.of(w, 1))))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(tp -> tp.f0)
                .sum("f1")
                .print();
        //输出
        //启动
        executionEnvironment.execute("WorkCount3");
    }
}
