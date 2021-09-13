package com.rookiex.day08;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * Side Output 旁路输出（侧流输出），就是跟主流打上标签，根据指定的标签取出想要的数据
 */
public class SideOutputDemo {

    public static void main(String[] args) throws Exception{

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入一些字符串，如果能转成Integer，并且打上奇数或偶数
        //1
        //2
        //3
        //abc 没法转成数字的，就打上字符串类型
        //4
        //5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //事先定义好指定的标签
        OutputTag<Integer> odd = new OutputTag<Integer>("odd-tag") {
        };
        OutputTag<Integer> even = new OutputTag<Integer>("even-tag") {
        };
        OutputTag<String> str = new OutputTag<String>("str-tag") {
        };

        SingleOutputStreamOperator<String> mainStream = lines.process(new ProcessFunction<String, String>() {
            @Override
            public void processElement(String in, Context ctx, Collector<String> out) throws Exception {

                try {
                    int i = Integer.parseInt(in);
                    if (i % 2 != 0) {
                        //奇数
                        //输出打标签的数据
                        ctx.output(odd, i);
                    } else {
                        //偶数
                        //输出打标签的数据
                        ctx.output(even, i);
                    }
                } catch (NumberFormatException e) {
                    //输出打标签的数据
                    ctx.output(str, in);
                }
                //输出未打标签的数据
                out.collect(in);
            }
        });

        mainStream.print("main -> ");

        DataStream<Integer> oddStream = mainStream.getSideOutput(even);

        DataStream<String> strStream = mainStream.getSideOutput(str);

        oddStream.print("odd -> ");

        strStream.print("str -> ");



        env.execute();

    }
}
