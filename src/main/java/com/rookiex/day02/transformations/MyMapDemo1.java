package com.rookiex.day02.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamMap;

/**
 * 不调用map方法，而是调用transform，实现与map相同的功能
 *
 */
public class MyMapDemo1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        //MapFunction是在JobManager端被new的
        MapFunction<String, String> func = new MapFunction<String, String>() {
            @Override
            public String map(String s) throws Exception {
                return s.toUpperCase();
            }
        };

        //调用Transformation，传入算子名称，返回类型，具体的运算逻辑
        SingleOutputStreamOperator<String> upperStream = words
                .transform("MyMap", TypeInformation.of(String.class), new StreamMap<>(func));

        upperStream.print();

        //启动并执行
        env.execute();





    }

}
