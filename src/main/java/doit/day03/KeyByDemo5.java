package doit.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class KeyByDemo5 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //辽宁省,沈阳市,1000
        //辽宁省,大连市,1000
        //山东省,济南市,1000
        //山东省,烟台市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

//        KeyedStream<Tuple3<String, String, Double>, String> keyed = tpDataStream.keyBy(new KeySelector<Tuple3<String, String, Double>, String>() {
//            @Override
//            public String getKey(Tuple3<String, String, Double> tp) throws Exception {
//                return tp.f0 + tp.f1;
//            }
//        });


//        KeyedStream<Tuple3<String, String, Double>, Tuple2<String, String>> keyed = tpDataStream.keyBy(new KeySelector<Tuple3<String, String, Double>, Tuple2<String, String>>() {
//            @Override
//            public Tuple2<String, String> getKey(Tuple3<String, String, Double> tp) throws Exception {
//                return Tuple2.of(tp.f0, tp.f1);
//            }
//        });

        //KeyedStream<Tuple3<String, String, Double>, String> keyed = tpDataStream.keyBy(tp -> tp.f0 + tp.f1);

//        KeyedStream<Tuple3<String, String, Double>, Tuple2<String, String>> keyed = tpDataStream.keyBy(
//                tp -> Tuple2.of(tp.f0, tp.f1),
//                TypeInformation.of(new TypeHint<Tuple2<String, String>>() {})
//        );


        //KeyedStream<Tuple3<String, String, Double>, Tuple> keyed = tpDataStream.keyBy(0, 1);

        KeyedStream<Tuple3<String, String, Double>, Tuple> keyed = tpDataStream.keyBy("f0", "f1");

        SingleOutputStreamOperator<Tuple3<String, String, Double>> summed = keyed.sum(2);

        summed.print();

        env.execute();

    }


}
