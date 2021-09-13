package com.rookiex.day04.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy并且按照ProcessingTime划分会话窗口
 * NonKeyed Window（并行度为1）
 */
public class ProcessingTimeSessionWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1
        //2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        AllWindowedStream<Integer, TimeWindow> window = nums.windowAll(ProcessingTimeSessionWindows.withGap(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> sum = window.sum(0);

        sum.print();

        env.execute();
    }

}
