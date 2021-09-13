package doit.day03;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class KeyByDemo2 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //spark
        //hadoop
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);


//        KeyedStream<String, String> keyed = words.keyBy(new KeySelector<String, String>() {
//            @Override
//            public String getKey(String word) throws Exception {
//                return word;
//            }
//        });

        KeyedStream<String, String> keyed = words.keyBy(w -> w);

        //使用字段位置下标的方式进行keyBy只适用于DataStream中对应的数据是Tuple类型
        //KeyedStream<String, Tuple> keyed = words.keyBy(0);

        keyed.print();

        env.execute();

    }
}
