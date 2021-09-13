package com.rookiex.day03.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * @Author RookieX
 * @Date 2021/8/20 9:28 上午
 * @Description:
 * Flink流式计算:DataStream 之后会生成 subTask, 每来一条数据, 调用一次 processElement 方法
 * 在该方法中, 应用外部出入的计算逻辑,
 */
public class MapDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        MapFunction<String, String> mapFunction = new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return value.toUpperCase() + "666";
            }
        };
        SingleOutputStreamOperator<String> upperDataStream = lines.transform(
                "MyMap",
                TypeInformation.of(new TypeHint<String>() {}),
               new MyStreamMap());
               // new StreamMap<>(mapFunction));//不传入 streamMap, 自定义一个

        upperDataStream.print();

        //启动并执行
        env.execute();

    }
    private static class MyStreamMap extends AbstractStreamOperator <String> implements OneInputStreamOperator<String, String> {

        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //从输入的 StreamRecord 中获取真实数据
            String value = element.getValue();
            //实现小写变大写
            String res = value.toUpperCase() + "666";
            StreamRecord<String> stringStreamRecord = new StreamRecord<>(res);
            output.collect(stringStreamRecord);
        }
    }
}
