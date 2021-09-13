package doit.day07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;

/**
 * 只有KeyedState可以设置TTL（Time To Live）即，数据存活时间
 *
 * 默认情况，KeyedState的TTL为一直存在
 */
public class KeyedStateTTLDemo1 {

    public static void main(String[] args) throws Exception {

        //编写Flink程序要创建一个StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //调用Transformation（s），开始
        //切分压平
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word); //输出
                }
            }
        });

        //单词将1组合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                //return new Tuple2<>(word, 1);
                return Tuple2.of(word, 1);
            }
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(t -> t.f0);

        //使用状态编程
        keyed.process(new KeyedProcessFunction<String, Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            //transient 状态不参与对象的序列化的反序列化
            private transient ValueState<Integer> counterState;

            @Override
            public void open(Configuration parameters) throws Exception {
                //初始化或恢复状态
                //使用状态的步骤：
                //1.定义一个状态描述器，描述状态的名称，描述状态的类型, 状态的TTL、状态的清理方式
                StateTtlConfig ttlConfig = StateTtlConfig
                        .newBuilder(Time.seconds(30))
                        //OnCreateAndWrite:  TTL时间的更新方式（默认的）创建或修改时会更新TTL的存活时间
                        //OnReadAndWrite:    使用这个状态（取值），也会修改TTL的存活时间
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        //NeverReturnExpired: 数据访问可见性（默认的）只要超时了，状态就不可以用了
                        //ReturnExpiredIfNotCleanedUp: 状态超时只要没有被清理也可以使用
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                ValueStateDescriptor<Integer> stateDescriptor = new ValueStateDescriptor<>("wc-state", Integer.class);
                //将状态描述器关联StateTtlConfig
                stateDescriptor.enableTimeToLive(ttlConfig);

                //2.使用RuntimeContext初始化或恢复状态
                counterState = getRuntimeContext().getState(stateDescriptor);
            }

            @Override
            public void processElement(Tuple2<String, Integer> tp, Context ctx, Collector<Tuple2<String, Integer>> out) throws Exception {

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

                out.collect(tp);

            }
        }).print();

        //启动
        env.execute("WordCount");


    }

}
