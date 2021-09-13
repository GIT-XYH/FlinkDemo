package com.rookiex.day02.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * //调用readTextFile方法，只传入读取数据的路径，那么该方法创建的DataStream使用一个有限的数据流
 * //数据读取完成，job就退出了
 */
public class FileSource {

    public static void main(String[] args) throws Exception{

        //创建DataStream，必须调用StreamExecutitionEnvriroment的方法
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());


        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //调用readTextFile方法，只传入读取数据的路径，那么该方法创建的DataStream使用一个有限的数据流
        //数据读取完成，job就退出了
        DataStreamSource<String> lines = env.readTextFile("/Users/xing/Desktop/a.txt");

        //Source的并行度
        int parallelism1 = lines.getParallelism();
        System.out.println("调用readTextFile方法得到的DataStream并行度为：" + parallelism1);

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
