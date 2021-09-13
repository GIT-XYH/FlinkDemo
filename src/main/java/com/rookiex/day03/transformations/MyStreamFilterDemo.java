package com.rookiex.day03.transformations;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

public class MyStreamFilterDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        //1
        //spark
        //2
        //5
        //4
        //hadoop
        //spark spark hadoop
        //(spark, 1)
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Integer> upperDataStream = lines.transform(
                "MyMap",
                TypeInformation.of(Integer.class),
                new MyStreamFilter()
        );

        upperDataStream.print();

        //启动并执行
        env.execute();


    }


    private static class MyStreamFilter extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<String, Integer> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            String in = element.getValue();
            try {
               int num = Integer.parseInt(in);
               if (num % 2 == 0) {
                   output.collect(element.replace(num));
               }
            } catch (NumberFormatException e) {}

        }
    }

}
