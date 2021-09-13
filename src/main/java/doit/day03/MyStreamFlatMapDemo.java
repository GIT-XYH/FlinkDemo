package doit.day03;

import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyStreamFlatMapDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //spark hadoop flink
        //(spark, 1)
        //(hadoop, 1)
        //(flink, 1)
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple2<String, Integer>> upperDataStream = lines.transform(
                "MyFlatMap",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),
                new MyStreamFlatMap()
        );

        upperDataStream.print();

        //启动并执行
        env.execute();


    }


    private static class MyStreamFlatMap extends AbstractStreamOperator<Tuple2<String, Integer>> implements OneInputStreamOperator<String, Tuple2<String, Integer>> {


        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //从输入StreamRecord中获取真实的数据
            String in = element.getValue();
            String[] words = in.split(" ");
            for (String word : words) {
                Tuple2<String, Integer> tp = Tuple2.of(word, 1);
                output.collect(element.replace(tp));
            }
        }
    }

}
