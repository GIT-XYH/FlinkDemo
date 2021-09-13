package doit.day06;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * 不开启checkpoint，并且希望Flink可以容错（我自己编程，将中间结果保存起来）
 * 不使用状态，而是在subtask中定义一个Integer的counter进行计算，没法记录每个单词的次数
 */
public class FaultToleranceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //设置规定次数的重启策略
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));

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
        });

        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(t -> t.f0);

        //对keyby之后的DataStream进行操作
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.map(new MapFunction<Tuple2<String, Integer>, Tuple2<String, Integer>>() {

            private Integer counter;

            @Override
            public Tuple2<String, Integer> map(Tuple2<String, Integer> tp) throws Exception {
                Integer current = tp.f1;
                if (counter == null) {
                    counter = 0;
                }
                counter += current;
                //更新
                tp.f1 = counter;
                //返回输出
                return tp;
            }
        });

        summed.print();

        env.execute();

    }
}
