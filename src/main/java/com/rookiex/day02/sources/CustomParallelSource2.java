package com.rookiex.day02.sources;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

/**
 * 继承RichParallelSourceFunction抽象类，创建的DataStream就是多并行
 *
 * 自定义一个parallel sources （多并行的Source）
 *
 * 这个例子是一个【无限】的数据流，run方法中会不停的产生数据（while循环）
 *
 * 在subtask中方法的执行顺序（生命周期方法） open(一次) -> run(一次) -> cancel（一次）-> close(一次)
 *
 */
public class CustomParallelSource2 {

    public static void main(String[] args) throws Exception {

        //创建DataStream，必须调用StreamExecutitionEnvriroment的方法
        //StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //设置整个job执行环境并行度为
        env.setParallelism(1);

        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);


        DataStreamSource<Integer> lines = env.addSource(new MyParallelSource2());
        int parallelism2 = lines.getParallelism();
        System.out.println("自定义的实现SourceFunction的Source并行度为：" + parallelism2);


        lines.print();

        env.execute();
    }

    //多并行的Source，继承了RichParallelSourceFunction
    // 即调用完Source后得到的DataStream中对应的数据类型
    private static class MyParallelSource2 extends RichParallelSourceFunction<Integer> {

        public MyParallelSource2() {
            System.out.println("constructor invoke ~~~~~~~~~~~~");
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            //getRuntimeContext().getIndexOfThisSubtask() 获取当前subTask的Index
            System.out.println("subtask：" + getRuntimeContext().getIndexOfThisSubtask() + " @@@@ open 方法被调用了");
            //super.open(parameters);
        }

        @Override
        public void close() throws Exception {
            System.out.println("subtask：" + getRuntimeContext().getIndexOfThisSubtask() + " &&&& close 方法被调用了");
            //super.close();
        }

        private boolean flag = true;
        /**
         * run方法是Source对应的Task启动后，会调用该方法，用来产生数据
         * 如果是一个【有限】的数据流，run方法中的逻辑执行完后，Source就停止了，整个job也停止了
         * 如果是一个【无限】的数据流，run方法中会有while循环，不停的产生数据
         *
         * 使用SourceContext将数据输出
         *
         * @param ctx
         * @throws Exception
         */
        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            System.out.println("subtask：" + getRuntimeContext().getIndexOfThisSubtask() + " Run方法被调用了~~~~~~~~~~~~~");
            int i = 0;
            while (flag) {
                ctx.collect(i);
                i += 1;
                Thread.sleep(2000);
            }
        }

        /**
         * 将程序停止的时候会调用cancel
         */
        @Override
        public void cancel() {
            System.out.println("subtask：" + getRuntimeContext().getIndexOfThisSubtask() + " cancel方法被调用了!!!!!!!!!!!!");
            flag = false;
        }
    }

}
