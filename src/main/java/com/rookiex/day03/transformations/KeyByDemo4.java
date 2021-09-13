package com.rookiex.day03.transformations;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class KeyByDemo4 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //spark
        //hadoop
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);


        SingleOutputStreamOperator<WordCount> wordAndOne = words.map(w -> new WordCount(w, 1));

//        KeyedStream<WordCount, String> keyed = wordAndOne.keyBy(new KeySelector<WordCount, String>() {
//            @Override
//            public String getKey(WordCount wc) throws Exception {
//                return wc.getWord();
//            }
//        });

        //KeyedStream<WordCount, String> keyed = wordAndOne.keyBy(wc -> wc.word);

        KeyedStream<WordCount, Tuple> keyed = wordAndOne.keyBy("word");


        SingleOutputStreamOperator<WordCount> summed = keyed.sum("count");

        summed.print();

        env.execute();

    }

    //用来封装数据的POJO，不能是private修饰
    public static class WordCount {

        private String word;
        private Integer count;

        // 用来封装数据的POJO，如果添加了有参的构造方法，必须添加无惨的构造方法
        public WordCount() {}

        public WordCount(String word, Integer count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        @Override
        public String toString() {
            return "WordCount{" +
                    "word='" + word + '\'' +
                    ", count=" + count +
                    '}';
        }
    }
}
