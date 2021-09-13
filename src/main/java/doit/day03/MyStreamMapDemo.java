package doit.day03;

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
 * Flink流式计算：DataStream以后会生成subTask，每来一条数据，调用一次processElement方法
 * 在该方法中，应用外部传入的计算逻辑，将返回的结果封装到StreamRecord中，然后使用Output输出，以供给后面的算子使用
 */

public class MyStreamMapDemo {


    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        //整个job的并行度
        int parallelism = env.getParallelism();
        System.out.println("当前job的执行环境默认的并行度为：" + parallelism);

        //DataStreamSource是DataStream的子类
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<String> upperDataStream = lines.transform(
                "MyMap",
                TypeInformation.of(new TypeHint<String>() {}),
                new MyStreamMap()
                //new StreamMap<>(mapFunction) //不传入默认的StreamMap，而是自定义一个
        );

        upperDataStream.print();

        //启动并执行
        env.execute();


    }


    private static class MyStreamMap extends AbstractStreamOperator<String> implements OneInputStreamOperator<String, String> {


        @Override
        public void processElement(StreamRecord<String> element) throws Exception {
            //从输入StreamRecord中获取真实的数据
            String in = element.getValue();
            String res = in.toUpperCase() + "666";
            StreamRecord<String> streamRecord = new StreamRecord<>(res);
            output.collect(streamRecord);
        }
    }

}
