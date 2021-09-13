package doit.day03;

import org.apache.flink.api.common.functions.MapFunction;
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
 * 数据过滤操作
 */
public class MyFilterMapDemo {
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
        SingleOutputStreamOperator<Integer> upperDataStream = lines.transform(
                "MyMap",
                TypeInformation.of(Integer.class),
               new MyStreamFilter());
               // new StreamMap<>(mapFunction));//不传入 streamMap, 自定义一个

        upperDataStream.print();
        //启动并执行
        env.execute();

    }
 private static class MyStreamFilter extends AbstractStreamOperator<Integer> implements OneInputStreamOperator<String, Integer> {

     @Override
     public void processElement(StreamRecord<String> element) throws Exception {
         String value = element.getValue();
         try {
             int parseInt = Integer.parseInt(value);
             if (parseInt % 2 == 0) {
                 output.collect(element.replace(parseInt));
             }
         } catch (NumberFormatException e) {
         }
     }
 }
}
