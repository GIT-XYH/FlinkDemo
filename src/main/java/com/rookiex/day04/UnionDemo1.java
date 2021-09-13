package com.rookiex.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 要Union的DataStream对应的数据类型必须一致
 */
public class UnionDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> nums = lines2.map(Integer::parseInt);

        //要union的多个DataStream对应的数据离线必须一致才可以
        //DataStream<String> union = lines1.union(nums);

        DataStream<String> union = lines1.union(lines2);

        union.print();

        env.execute();

    }
}
