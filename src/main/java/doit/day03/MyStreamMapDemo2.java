package doit.day03;

import org.apache.flink.api.common.functions.MapFunction;
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

public class MyStreamMapDemo2 {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        MapFunction<String, Tuple2<String, Integer>> mapFunction = new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String w) throws Exception {
                Tuple2<String, Integer> tp = Tuple2.of(w, 1);
                return tp;
            }
        };


        SingleOutputStreamOperator<Tuple2<String, Integer>> upperDataStream = lines.transform(
                "MyMap",
                TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}),
                new MyStreamMap2(mapFunction)
        );

        upperDataStream.print();

        //启动并执行
        env.execute();


    }


    private static class MyStreamMap2 extends AbstractStreamOperator<Tuple2<String, Integer>> implements OneInputStreamOperator<String, Tuple2<String, Integer>> {

        private MapFunction<String, Tuple2<String, Integer>> mapFunction;

        public MyStreamMap2(MapFunction<String, Tuple2<String, Integer>> mapFunction) {
            this.mapFunction = mapFunction;
        }

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //String in = element.getValue();
            //Tuple2<String, Integer> tp = mapFunction.map(in);
            //output.collect(element.replace(tp));
            int a = 0;
            output.collect(element.replace(mapFunction.map(element.getValue())));
        }
    }

}
