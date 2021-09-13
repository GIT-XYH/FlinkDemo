package com.rookiex.day02.sources;

import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class SocketSource {

    public static void main(String[] args) throws Exception{

        //创建DataStream，必须调用StreamExecutitionEnvriroment的方法
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //Source的并行度
        int parallelism1 = lines.getParallelism();
        System.out.println("调用socketTextStream方法得到的DataStream并行度为：" + parallelism1);


        //可以调用Transformation（s）

        //调用Sink
        DataStreamSink<String> printSink = lines.print();

        //Sink的并行度
        int parallelism2 = printSink.getTransformation().getParallelism();
        System.out.println("PrintSink的并行度为：" + parallelism2);


        //启动并执行
        env.execute();
    }
}
