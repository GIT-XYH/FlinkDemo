package doit.day07;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class KafkaToRedisWordCount {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //为了容错，要开启checkpoint
        env.enableCheckpointing(15000);
        //在checkpoint时，将状态保存到HDFS存储后端
        env.setStateBackend(new FsStateBackend("hdfs://node-1.51doit.cn:9000/fchk24"));
        //将job cancel后，保留外部存储的checkpoint数据（为了以后重新提交job恢复数据）
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "node-1.51doit.cn:9092,node-2.51doit.cn:9092,node-3.51doit.cn:9092");
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("group.id", args[1]);

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                args[0],
                new SimpleStringSchema(),
                properties
        );

        //在checkpoint成功后，不将偏移量写入到Kafka特殊的topic中
        kafkaConsumer.setCommitOffsetsOnCheckpoints(false);

        DataStreamSource<String> lines = env.addSource(kafkaConsumer);

        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = lines.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String line, Collector<Tuple2<String, Integer>> out) throws Exception {
                for (String w : line.split(",")) {
                    out.collect(Tuple2.of(w, 1));
                }
            }
        });

        SingleOutputStreamOperator<Tuple2<String, Integer>> summed = wordAndOne.keyBy(t -> t.f0).sum(1);

        FlinkJedisPoolConfig redisConf = new FlinkJedisPoolConfig
                .Builder()
                .setHost("node-3.51doit.cn")
                .setPassword("123456")
                .setDatabase(6)
                .build();


        //将数据写入到Redis
        summed.addSink(new RedisSink<Tuple2<String, Integer>>(redisConf, new RedisWordCountMapper()));

        env.execute();
    }

    public static class RedisWordCountMapper implements RedisMapper<Tuple2<String, Integer>> {

        //以何种方式写入到Redis中
        @Override
        public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.HSET, "WORD_COUNT");
        }

        @Override
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0; //将单词作为key
        }

        @Override
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }
}
