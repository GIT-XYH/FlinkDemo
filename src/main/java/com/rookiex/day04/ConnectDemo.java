package com.rookiex.day04;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;

/**
 * Connect可以将两个类型不一样的流connect到一起
 * 连接后生成一个新的DataStream，包装者原来的两个流
 * 重要功能：Connect后，两个流可以共享状态
 */
public class ConnectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Integer> nums2 = lines2.map(Integer::parseInt).setParallelism(3);

        //String类型的流connect Integer类型的流
        ConnectedStreams<String, Integer> connected = lines1.connect(nums2);

        SingleOutputStreamOperator<String> res = connected.map(new CoMapFunction<String, Integer, String>() {

            //private FlinkState state;

            //对第一流进行操作的方法
            @Override
            public String map1(String value) throws Exception {
                //map1可以使用state
                return value.toUpperCase();
            }

            //是对第二个流继续操作的方法
            @Override
            public String map2(Integer value) throws Exception {
                //map2可以使用state
                return value + " doit";
            }
        });

        res.print();

        env.execute();

    }
}
