package com.rookiex.day01;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @Author RookieX
 * @Date 2021/8/18 11:36 上午
 * @Description:
 */
public class WordCount {
    public static void main(String[] args) throws Exception {
        //编写 spark 程序要创建一个 sparkContext
        //编写 flink 程序要编写 StreamExecutionEnvironment
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //创建抽象数据集, source用来读取数据
        //spark hadoop flink spark
        //Source
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //调用 Transformation, 可以一次或多次, 用来转化数据
        SingleOutputStreamOperator<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
            //输出的一行数据, 多行就放到 collector 中
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] words = line.split(" ");
                for (String word : words) {
                    collector.collect(word);//输出数据, 返回的是一个 DS
                }
            }
        });

        //将单词和 1 组合
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
//                return new Tuple2<>(word, 1);
                return Tuple2.of(word, 1);
            }
        });

        //分区
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndOne.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tp) throws Exception {
                return tp.f0;
            }
        });

        //聚合
        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = keyed.sum(1);
        //TransFormation 结束

        //调用 Data Sink
        summed.print();

        //启动程序
        env.execute("WordCount");

    }
}
