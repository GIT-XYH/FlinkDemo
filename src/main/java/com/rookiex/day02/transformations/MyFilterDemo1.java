package com.rookiex.day02.transformations;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.StreamFilter;
import org.apache.flink.streaming.api.operators.StreamMap;

/**
 * 不调用filter方法，而是调用transform，实现与filter相同的功能
 *
 */
public class MyFilterDemo1 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        FilterFunction<String> filterFunction = new FilterFunction<String>() {
            @Override
            public boolean filter(String s) throws Exception {
                return s.startsWith("h");
            }
        };

        SingleOutputStreamOperator<String> filtered = words.transform("MyFilter", TypeInformation.of(String.class), new StreamFilter<>(filterFunction));

        filtered.print();

        //启动并执行
        env.execute();





    }

}
