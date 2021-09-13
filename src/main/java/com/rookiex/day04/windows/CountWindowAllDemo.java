package com.rookiex.day04.windows;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;

/**
 * 按照条数划分窗口
 *
 * 按照有没有分区的DataStream划分窗口
 *  Keyed Window，先KeyBy再划分窗口（第一种用的最多）
 *  NonKeyed Window 没有keyBy就划分窗口（几乎很少用）
 *
 *  NonKeyed Window不论怎样改变程序的并行度，NonKeyed Window的并行度永远都为1
 */
//不KeyBy就划分窗口
public class CountWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //1
        //2
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> nums = lines.map(Integer::parseInt);

        //按照条数划分窗口
        //不KeyBY就划分窗口（NonKeyWindow）
        AllWindowedStream<Integer, GlobalWindow> windowAll = nums.countWindowAll(5);

        //对窗口中的数据进行运算
        SingleOutputStreamOperator<Integer> res = windowAll.sum(0);

        res.print();

        env.execute();
    }

}
