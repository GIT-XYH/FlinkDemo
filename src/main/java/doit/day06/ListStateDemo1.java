package doit.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

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
public class ListStateDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint功能
        env.enableCheckpointing(5000);

        //spark,5
        //spark,7
        //spark,3
        //spark,8
        //hive,5
        //hive,10
        //hive,8
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = lines.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple2.of(fields[0], Integer.parseInt(fields[1]));
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyedStream = wordAndCount.keyBy(t -> t.f0);

        keyedStream.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private transient ListState<Integer> listState;

            @Override
            public void open(Configuration parameters) throws Exception {
                ListStateDescriptor<Integer> stateDescriptor = new ListStateDescriptor<>("lst-state", Integer.class);
                listState = getRuntimeContext().getListState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> value, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

                List<Integer> tempList = new ArrayList<>();

                Integer count = value.f1;
                listState.add(count);

                Iterable<Integer> lst = listState.get();
                for (Integer i : lst) {
                    tempList.add(i);
                }
                tempList.sort(new Comparator<Integer>() {
                    @Override
                    public int compare(Integer o1, Integer o2) {
                        return o2 - o1;
                    }
                });
                List<Integer> resList = tempList.subList(0, Math.min(tempList.size(), 3));
                listState.clear();
                listState.addAll(resList);

                for (Integer integer : resList) {
                    out.collect(Tuple2.of(value.f0, integer));
                }


            }
        }).print();


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
