package doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class BroadcastStateDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //oid01,cid01,1000
        DataStreamSource<String> orderStream = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> orderTpStream = orderStream.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //cid01,手机
        DataStreamSource<String> categoryStream = env.socketTextStream("localhost", 9999);

        //维度数据流
        SingleOutputStreamOperator<Tuple2<String, String>> categoryTpStream = categoryStream.map(new MapFunction<String, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], fields[1]);
            }
        });

        //将维度数据流进行广播（使用BroadcastState）
        //定义状态描述器（MapStateDescriptor）
        MapStateDescriptor<String, String> categoryStateDescriptor = new MapStateDescriptor<>("category-state", String.class, String.class);
        //广播并制定对应的StateDescriptor
        BroadcastStream<Tuple2<String, String>> broadcastStream = categoryTpStream.broadcast(categoryStateDescriptor);

        //主流（订单流）connect广播的流
        BroadcastConnectedStream<Tuple3<String, String, Double>, Tuple2<String, String>> connectedStream = orderTpStream.connect(broadcastStream);

        connectedStream.process(new BroadcastProcessFunction<Tuple3<String, String, Double>, Tuple2<String, String>, Tuple4<String, String, String, Double>>() {

            @Override
            public void processElement(Tuple3<String, String, Double> input, ReadOnlyContext ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {
                String cid = input.f1;
                ReadOnlyBroadcastState<String, String> broadcastState = ctx.getBroadcastState(categoryStateDescriptor);
                String name = broadcastState.get(cid);

                out.collect(Tuple4.of(input.f0, cid, name, input.f2));
            }

            @Override
            public void processBroadcastElement(Tuple2<String, String> input, Context ctx, Collector<Tuple4<String, String, String, Double>> out) throws Exception {
                String cid = input.f0;
                String name = input.f1;
                //broadcastState底层是一个map类型
                BroadcastState<String, String> broadcastState = ctx.getBroadcastState(categoryStateDescriptor);
                broadcastState.put(cid, name);
            }
        }).print();

        env.execute();


    }
}
