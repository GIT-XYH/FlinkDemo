package com.rookiex.day02.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @Author RookieX
 * @Date 2021/8/19 4:38 下午
 * @Description:
 */
    public class MapDemo {

        public static void main(String[] args) throws Exception{

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
            //整个job的并行度
            int parallelism = env.getParallelism();
            System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

            //DataStreamSource是DataStream的子类
            DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

            //做映射
            //调用完map算子后生成的DataStream是单并行的还是多并行的？答案：多并行的
            SingleOutputStreamOperator<String> upperDataStream = lines.map(new MapFunction<String, String>() {
                @Override
                public String map(String s) throws Exception {
                    return s.toUpperCase();
                }
            }).setParallelism(2);

            System.out.println("map算子对应的DataStream的并行度：" + upperDataStream.getParallelism());


            upperDataStream.print();

            //启动并执行
            env.execute();





        }
}
