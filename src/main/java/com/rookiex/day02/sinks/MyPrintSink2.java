package com.rookiex.day02.sinks;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

/**
 * 自定义的PrintSink，并且前面带上Subtask Index + 1
 */
public class MyPrintSink2 {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        lines.addSink(new MyPrintSink()).name("MyPrintSink");
        env.execute();
    }

    private static class MyPrintSink extends RichSinkFunction<String> {

        @Override
        public void open(Configuration parameters) throws Exception {
            System.out.println("open method invoked ~~~~~~~");
        }

        @Override
        public void invoke(String value, Context context) throws Exception {
            System.out.println("invoke method invoked $$$$$$$$$$");
            //获取当前subtask的index
            int indexOfThisSubtask = getRuntimeContext().getIndexOfThisSubtask();

            System.out.println((indexOfThisSubtask + 1) + "> " + value);
        }

        @Override
        public void close() throws Exception {
            System.out.println("close method invoked ########");
        }
    }

}
