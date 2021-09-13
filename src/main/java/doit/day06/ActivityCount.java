package doit.day06;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashSet;

public class ActivityCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        SingleOutputStreamOperator<Tuple3<String, String, String>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, String>>() {
            @Override
            public Tuple3<String, String, String> map(String line) throws Exception {
                String[] fields = line.split(",");
                //uid,aid,type
                return Tuple3.of(fields[0], fields[1], fields[2]);
            }
        });

        KeyedStream<Tuple3<String, String, String>, Tuple2<String, String>> keyedStream = tpStream.keyBy(new KeySelector<Tuple3<String, String, String>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, String> tp) throws Exception {
                return Tuple2.of(tp.f1, tp.f2);
            }
        });

        keyedStream.process(new KeyedProcessFunction<Tuple2<String, String>, Tuple3<String, String, String>, Tuple4<String, String, Integer, Integer>>() {

            private transient ValueState<Integer> countState;
            private transient ValueState<HashSet<String>> uidState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ValueStateDescriptor<Integer> countStateDescriptor = new ValueStateDescriptor<>("count-state", Integer.class);
                countState = getRuntimeContext().getState(countStateDescriptor);

                ValueStateDescriptor<HashSet<String>> uidStateDescriptor = new ValueStateDescriptor<>("uid-state", TypeInformation.of(new TypeHint<HashSet<String>>() {}));
                uidState = getRuntimeContext().getState(uidStateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, String> input, Context ctx, Collector<Tuple4<String, String, Integer, Integer>> out) throws Exception {
                String uid = input.f0;
                //String aid = input.f1;
                //String type = input.f2;
                Integer history = countState.value();
                if(history == null) {
                    history = 0;
                }
                history += 1;
                countState.update(history);

                //计算人数（去重）
                HashSet<String> uids = uidState.value();
                if(uids == null) {
                    uids = new HashSet<>();
                }
                uids.add(uid);
                //更新状态
                uidState.update(uids);

                //输出结果
                out.collect(Tuple4.of(input.f1, input.f2, uids.size(), history));
            }
        }).print();

        env.execute();

    }
}
