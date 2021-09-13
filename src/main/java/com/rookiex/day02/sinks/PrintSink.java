package com.rookiex.day02.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将数据以标准数据大于，PrintSink是多并行的
 */
public class PrintSink {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.print("doit ");

        //启动并执行
        env.execute();
    }


}
