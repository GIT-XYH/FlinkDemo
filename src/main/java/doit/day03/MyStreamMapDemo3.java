package doit.day03;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyStreamMapDemo3 {


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
                new MyStreamMap3(mapFunction)
        );

        upperDataStream.print();

        //启动并执行
        env.execute();


    }


    private static class MyStreamMap3<I, O> extends AbstractUdfStreamOperator<O, MapFunction<I, O>> implements OneInputStreamOperator<I, O> {



        public MyStreamMap3(MapFunction<I, O> mapFunction) {
            super(mapFunction);
        }

        @Override
        public void processElement(StreamRecord<I> element) throws Exception {
            output.collect(element.replace(userFunction.map(element.getValue())));
        }
    }
}
