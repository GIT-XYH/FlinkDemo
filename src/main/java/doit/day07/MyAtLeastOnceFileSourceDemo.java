package doit.day07;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MyAtLeastOnceFileSourceDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        env.enableCheckpointing(10000);

        env.setParallelism(4);

        DataStreamSource<String> lines = env.addSource(new MyAtLeastOnceFileSource("/Users/xing/Desktop/data"));

        DataStreamSource<String> socketLines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> errorLines = socketLines.map(new MapFunction<String, String>() {
            @Override
            public String map(String line) throws Exception {
                if (line.startsWith("error")) {
                    throw new RuntimeException("有问题数据，抛出异常！");
                }
                return line;
            }
        });

        //让两个流关联到一起，目的是为了在socket流人为的输入错误数据，让整个流重启
        DataStream<String> union = lines.union(errorLines);

        union.print();

        env.execute();


    }
}
