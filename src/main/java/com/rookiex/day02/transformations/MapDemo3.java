package com.rookiex.day02.transformations;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class MapDemo3 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> words = env.socketTextStream("localhost", 8888);

//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words.map(new MapFunction<String, Tuple2<String, Integer>>() {
//            @Override
//            public Tuple2<String, Integer> map(String s) throws Exception {
//                return Tuple2.of(s, 1);
//            }
//        });

        //使用lambda表达式，替代匿名内部类
        //如果输入数据类型跟返回的数据类型不一致，就要使用return指定类型
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words
                .map(w -> Tuple2.of(w, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT));


//        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndOne = words
//                .map(w -> Tuple2.of(w, 1), TypeInformation.of(new TypeHint<Tuple2<String, Integer>>() {}));


        wordAndOne.print();

        //启动并执行
        env.execute();





    }

    public static class MapDemo2 {

        public static void main(String[] args) throws Exception{

            StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
            //整个job的并行度
            int parallelism = env.getParallelism();
            System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

            //DataStreamSource是DataStream的子类
            DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

            //做映射
            //调用完map算子后生成的DataStream是单并行的还是多并行的？答案：多并行的
            //SingleOutputStreamOperator<String> upperDataStream = lines.map(s -> s.toUpperCase()).setParallelism(2);
            //使用lambda表达式，输入的类型跟输出的类型一样，就可以不加returns
            SingleOutputStreamOperator<String> upperDataStream = lines.map(String::toUpperCase).setParallelism(2);

            System.out.println("map算子对应的DataStream的并行度：" + upperDataStream.getParallelism());


            upperDataStream.print();

            //启动并执行
            env.execute();





        }

    }
}
