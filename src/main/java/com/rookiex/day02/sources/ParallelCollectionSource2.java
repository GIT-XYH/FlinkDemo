package com.rookiex.day02.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 将集合变成DataStream
 *
 * 集合类的Source都是有限数据流，即将集合内的数据读完后，就停止了
 *
 * generateSequence 创建的DataStream并行度可以是多个
 */
public class ParallelCollectionSource2 {

    public static void main(String[] args) throws Exception{

        //创建DataStream，必须调用StreamExecutitionEnvriroment的方法
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource<LongValue> lines = env.fromParallelCollection(new LongValueSequenceIterator(1L, 10L), LongValue.class);
        DataStreamSource<Long> nums = env.generateSequence(1, 10);

        //Source的并行度
        int parallelism1 = nums.getParallelism();
        System.out.println("调用readTextFile方法得到的DataStream并行度为：" + parallelism1);

        //可以调用Transformation（s）

        //调用Sink
        DataStreamSink<Long> printSink = nums.print();

        //Sink的并行度
        int parallelism2 = printSink.getTransformation().getParallelism();
        System.out.println("PrintSink的并行度为：" + parallelism2);


        //启动并执行
        env.execute();
    }
}
