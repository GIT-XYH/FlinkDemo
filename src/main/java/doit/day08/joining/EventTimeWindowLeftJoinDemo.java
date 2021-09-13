package doit.day08.joining;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.CoGroupFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.time.Duration;

/**
 * 现在演示的是窗口的LeftJoin
 *
 */
public class EventTimeWindowLeftJoinDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //订单主表（订单ID，订单总金额，订单状态）
        //2000,o100,2000,已下单
        //5002,o101,2000,已下单
        //10000,o102,2000,已下单
        DataStreamSource<String> lines1 = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> line1WithWaterMark = lines1.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0])));

        SingleOutputStreamOperator<Tuple3<String, Double, String>> orderMainStream = line1WithWaterMark.map(new MapFunction<String, Tuple3<String, Double, String>>() {
            @Override
            public Tuple3<String, Double, String> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple3.of(fields[1], Double.parseDouble(fields[2]), fields[3]);
            }
        });

        //订单明细表（订单主表ID，分类，单价，数量）
        //1000,o100,手机,500,2
        //5001,o100,电脑,1000,1
        //10003,o101,电脑,1000,1
        DataStreamSource<String> lines2 = env.socketTextStream("localhost", 9999);

        SingleOutputStreamOperator<String> line2WithWaterMark = lines2.assignTimestampsAndWatermarks(WatermarkStrategy.<String>forBoundedOutOfOrderness(Duration.ZERO).withTimestampAssigner((line, timestamp) -> Long.parseLong(line.split(",")[0])));


        SingleOutputStreamOperator<Tuple4<String, String, Double, Integer>> orderDetailStream = line2WithWaterMark.map(new MapFunction<String, Tuple4<String, String, Double, Integer>>() {
            @Override
            public Tuple4<String, String, Double, Integer> map(String value) throws Exception {
                String[] fields = value.split(",");
                return Tuple4.of(fields[1], fields[2], Double.parseDouble(fields[3]), Integer.parseInt(fields[4]));
            }
        });

        //将orderDetailStream当成左表
        //Leftjoin后的数据
        //o100,手机,500,2,已下单
        //o100,电脑,1000,1,null

        //join或CoGroup的条件，必须类型相同并且等值的
        DataStream<Tuple5<String, String, String, Double, Integer>> joined = orderDetailStream.coGroup(orderMainStream)
                .where(f -> f.f0)
                .equalTo(s -> s.f0)
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .apply(new CoGroupFunction<Tuple4<String, String, Double, Integer>, Tuple3<String, Double, String>, Tuple5<String, String, String, Double, Integer>>() {
                    @Override
                    public void coGroup(Iterable<Tuple4<String, String, Double, Integer>> first, Iterable<Tuple3<String, Double, String>> second, Collector<Tuple5<String, String, String, Double, Integer>> out) throws Exception {
                        //窗口触发，并且join的条件形同
                        for (Tuple4<String, String, Double, Integer> tp1 : first) {
                            boolean isJoined = false;
                            //左表中有数据
                            for (Tuple3<String, Double, String> tp2 : second) {
                                isJoined = true;
                                out.collect(Tuple5.of(tp1.f0, tp2.f2, tp1.f1, tp1.f2, tp1.f3));
                            }
                            if (!isJoined) {
                                out.collect(Tuple5.of(tp1.f0, null, tp1.f1, tp1.f2, tp1.f3));
                            }
                        }
                    }
                });

        joined.print();


        env.execute();

    }
}

