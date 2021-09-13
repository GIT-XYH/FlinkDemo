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
 * 设置共享资源槽
 */
public class SlotSharingGroupDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> lines = env.socketTextStream(args[0], 8888);

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
        }).slotSharingGroup("doit"); //将DataStream对应的subtask打上标签
        //默认情况，资源槽的名称为default，如果调用slotSharingGroup设置了名从，对应的subtask的共享资源槽的名称就改变了
        //不同资源槽名称的subtask不能进入到同一个槽位中
        //slotSharingGroup该方法调用后，后面的算子的共享资源槽名称都跟随着改变(就近跟随原则)

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = filtered.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        }).slotSharingGroup("default");

        wordAndOne.keyBy(t -> t.f0).sum(1).print();

        env.execute();
    }
}
