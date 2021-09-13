package com.rookiex.day02.transformations;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Arrays;

public class FlatMapDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> wordsDataStream = lines.flatMap((String line, Collector<String> out) -> {
            String[] words = line.split(" ");
            for (String word : words) {
                out.collect(word);
            }
        }).returns(Types.STRING);

        wordsDataStream.print();

        //启动并执行
        env.execute();

    }

    private static class MyMapFunction extends RichMapFunction<String, Tuple2<String, Integer>> {

        private Connection connection;

        @Override
        public void open(Configuration parameters) throws Exception {
            //查询数据库
            //创建数据流连接
            connection = DriverManager.getConnection("", "", "");
            //super.open(parameters);
        }

        @Override
        public Tuple2<String, Integer> map(String s) throws Exception {
            //getRuntimeContext()
            //使用connection窗PrepareStatement
            return null;
        }

        @Override
        public void close() throws Exception {
            //super.close();
            connection.close();
        }

    }
}

