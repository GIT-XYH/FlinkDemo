package com.rookiex.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 要Union的DataStream对应的数据类型必须一致
 */
public class UnionDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> num1 = lines1.map(Integer::parseInt).setParallelism(2);

        SingleOutputStreamOperator<Integer> nums2 = lines2.map(Integer::parseInt).setParallelism(3);

        DataStream<Integer> union = num1.union(nums2);

        union.print();

        env.execute();

    }
}
