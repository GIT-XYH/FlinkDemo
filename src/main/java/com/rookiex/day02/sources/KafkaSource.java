package com.rookiex.day02.sources;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author RookieX
 * @Date 2021/8/19 2:43 下午
 * @Description:
 * 使用 FlinkKafkaSource 从 Kafka 中读取数据
 * FlinkKafkaSource 创建的 DataStream 是多并行的, 可以有多个消费者从 Kafka 中读取数据
 */
public class KafkaSource {
    public static void main(String[] args) throws Exception {
        //创建DataStream，必须调用StreamExecutionEnvironment的方法
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //整个job的并行度
        int parallelism = env.getParallelism();
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "rookiex01:9092,rookiex02:9092,rookiex03:9092");
        properties.setProperty("auto.offset", "earliest");
        properties.setProperty("group.id", "test");
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);
        FlinkKafkaConsumer<String> flinkKafkaConsumer = new FlinkKafkaConsumer<>(
                "wordCount", new SimpleStringSchema(), properties
        );

        DataStreamSource<String> lines = env.addSource(flinkKafkaConsumer);
        //Source的并行度
        int parallelism1 = lines.getParallelism();
        System.out.println("调用flinkKafkaConsumer方法得到的DataStream并行度为：" + parallelism1);


        //可以调用Transformation（s）

        //调用Sink
        DataStreamSink<String> printSink = lines.print();

        //Sink的并行度
        int parallelism2 = printSink.getTransformation().getParallelism();
        System.out.println("PrintSink的并行度为：" + parallelism2);


        //启动并执行
        env.execute();

    }
}
