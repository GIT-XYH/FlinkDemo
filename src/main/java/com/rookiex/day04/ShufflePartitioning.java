package com.rookiex.day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 在Flink中shuffle是一个方法，是将数据随机分到下游的分区
 * shuffle是随机计算出一个分区编号（channel编号），然后下游到上游拉取数据
 */
public class ShufflePartitioning {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //并行度为1
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> mapped = words.map(new RichMapFunction<String, String>() {
            @Override
            public String map(String w) throws Exception {
                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();
                return w + " -> " + indexOfThisSubtask;
            }
        }).setParallelism(1);

        //对数据进行shuffle（将上游的数据随机打散，本质上是生成一个随机的分区号）
        DataStream<String> shuffled = mapped.shuffle();

        shuffled.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {

                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

                System.out.println(value + " -> " + indexOfThisSubtask);
            }
        });



        env.execute();

    }
}
