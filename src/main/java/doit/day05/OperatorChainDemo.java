package doit.day05;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * disableOperatorChaining 禁用所用的算子链
 */
public class OperatorChainDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //在StreamExecutionEnvironment禁用算子链
        //禁用算子链非常愚蠢，几乎不使用
        env.disableOperatorChaining();

        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                for (String word : line.split(" ")) {
                    collector.collect(word);
                }
            }
        });

        SingleOutputStreamOperator<String> filtered = words.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String word) throws Exception {
                return word.startsWith("h");
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        wordAndOne.keyBy(t -> t.f0).sum(1).print();

        env.execute();
    }
}
