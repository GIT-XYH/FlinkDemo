package doit.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 使用Flink的状态编程API
 * Flink状态分类两种
 *  - KeyedState（调用keyBy后使用的状态，有分组的状态）
 *      - ValueState (K, V)
 *      - MapState   (K, (k, v))
 *      - ListState  (K, List(v1, v2))
 *  - OperatorState（没有KeyBy，没有分组的状态）
 *      - ListState List(v1, v2)
 *      - BroadcastState
 */
public class ValueStateDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint功能
        env.enableCheckpointing(5000);

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    if("error".equals(word)) {
                        throw new RuntimeException("有问题数据，出异常了！");
                    }
                    collector.collect(word);
                }
            }
        });

        ;
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).slotSharingGroup("default");

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndOne.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyedStream.map(new MyReduceFunction());

        summed.print();

        env.execute();

    }


    private static class MyReduceFunction extends RichMapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>> {

        //transient 状态不参与对象的序列化的反序列化
        private transient ValueState<Integer> counterState;

        @Override
        public void open(Configuration parameters) throws Exception {
            //初始化或恢复状态
            //使用状态的步骤：
            //1.定义一个状态描述器，描述状态的名称，描述状态的类型
            ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-state", Integer.class);
            //2.使用RuntimeContext初始化或恢复状态
            counterState = getRuntimeContext().getState(stateDescriptor);
        }

        @Override
        public Tuple2<String, Integer> map(Tuple2<String, Integer> tp) throws Exception {
            Integer current = tp.f1;
            Integer history = counterState.value();
            if (history == null) {
                history = 0;
            }
            int sum = history + current;
            //更新状态
            counterState.update(sum);

            tp.f1 = sum;
            //输出结果
            return tp;
        }
    }

}
