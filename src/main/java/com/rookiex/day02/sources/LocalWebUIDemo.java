package com.rookiex.day02.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 本地模式开启WebUI，可以看到job的运行状态等信息
 *
 * 1.引入依赖
 *
 *  <dependency>
 *      <groupId>org.apache.flink</groupId>
 *      <artifactId>flink-runtime-web_${scala.binary.version}</artifactId>
 *      <version>${flink.version}</version>
 *  </dependency>
 *
 * 2.创建特殊StreamExecutionEnvironment （local模式带UI的）
 *
 * StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
 *
 */
public class LocalWebUIDemo {

    public static void main(String[] args) throws Exception{

        //创建DataStream，必须调用StreamExecutitionEnvriroment的方法
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        Configuration configuration = new Configuration();
        configuration.setInteger("rest.port", 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(configuration);

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
