package doit.day08.joining;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * 为什么要让数据流进行join
 *
 * 因为想要的数据来自两个流，必须将两个流管理到一起，才能获取全部的数据
 *
 * 怎样才能让两个流join到一起呢？
 *   第一种：划分窗口（让两个流数量暂时停了在一个窗口内），然后根据相同的条件，进行关联
 *   第二中：将两个流connect到一起（共享状态），将每个流数据存储到状态中，并且设置状态的TTL
 *
 *
 * 现在演示的是窗口的JOIN
 *
 */
public class WindowJoinDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //订单主表（订单ID，订单总金额，订单状态）
        //o100,2000,已下单
        //o101,2000,已下单
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, Double, String>> orderMainStream = lines1.map(new MapFunction<String, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[0], Double.parseDouble(fields[1]), fields[2]);
            }
        });

        //订单明细表（订单主表ID，分类，单价，数量）
        //o100,手机,500,2
        //o100,电脑,1000,1
        //o101,电脑,2000,1
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<Tuple4<String, String, Double, Integer>> orderDetailStream = lines2.map(new MapFunction<String, Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple4.of(fields[0], fields[1], Double.parseDouble(fields[2]), Integer.parseInt(fields[3]));
            }
        });

        //join后的数据
        //o100,已下单,手机,500,2
        //o100,已下单,电脑,1000,1

        DataStream<Tuple5<String, String, String, Double, Integer>> joinedStream = orderMainStream.join(orderDetailStream)
                .where(t -> t.f0)
                .equalTo(t -> t.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(new JoinFunction<Tuple3<String, Double, String>, Tuple4<String, String, Double, Integer>, Tuple5<String, String, String, Double, Integer>>() {
                    @Override
                    public Tuple5<String, String, String, Double, Integer> join(Tuple3<String, Double, String> first, Tuple4<String, String, Double, Integer> second) throws Exception {
                        //窗口触发是，并且join的条件相同的，才会执行join方法
                        return Tuple5.of(first.f0, first.f2, second.f1, second.f2, second.f3);
                    }
                });

        joinedStream.print();

        env.execute();

    }
}
