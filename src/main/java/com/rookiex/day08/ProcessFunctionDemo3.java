package com.rookiex.day08;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 对 keyedStream 使用 ProcessFunction
 * 使用状态 State
 */
public class ProcessFunctionDemo3 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //开启Checkpoint功能
        env.enableCheckpointing(5000);

        //将省份相同的是分到一个区中，并且将城市相同的进行聚合
        //辽宁省,沈阳市,1000
        //辽宁省,大连市,2000
        //辽宁省,沈阳市,3000
        //辽宁省,大连市,4000
        //河北省,廊坊市,1000
        //河北省,石家庄市,2000
        //河北省,廊坊市,2000
        //河北省,石家庄市,2000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                if (line.startsWith("error")) {
                    int i = 1 / 0;
                }
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //按照省份进行keyBye
        KeyedStream<Tuple3<String, String, Double>, String> keyed = tpStream.keyBy(t -> t.f0);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> reduced = keyed.process(new KeyedProcessFunction<String, Tuple3<String, String, Double>, Tuple3<String, String, Double>>() {

            private transient MapState<String, Double> mapState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //定义状描述器
                MapStateDescriptor<String, Double> stateDescriptor = new MapStateDescriptor<>("income-state", String.class, Double.class);
                //使用RuntimeContext获取状态
                mapState = getRuntimeContext().getMapState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple3<String, String, Double> input, Context ctx, Collector<Tuple3<String, String, Double>> out) throws Exception {
                String city = input.f1;
                Double current = input.f2;
                Double history = mapState.get(city);
                if (history == null) {
                    history = 0.0;
                }
                history += current;
                //更新到状态中
                mapState.put(city, history);
                //输出数据
                input.f2 = history;
                out.collect(input);
            }
        });

        reduced.print();

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
