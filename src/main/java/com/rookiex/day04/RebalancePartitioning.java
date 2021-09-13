package com.rookiex.day04;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 轮询分区
 */
public class RebalancePartitioning {

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

        //轮询：1 -> 2 -> 3 -> 0 -> 1 -> 2
        DataStream<String> rebalanced = mapped.rebalance();

        rebalanced.addSink(new RichSinkFunction<String>() {
            @Override
            public void invoke(String value, Context context) throws Exception {

                int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

                System.out.println(value + " -> " + indexOfThisSubtask);
            }
        });



        env.execute();

    }
}
