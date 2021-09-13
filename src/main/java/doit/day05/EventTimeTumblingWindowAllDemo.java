package doit.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 没有KeyBy并且按照EventTime划分滚动窗口
 * NonKeyed Window（并行度为1）
 *
 * 使用老版本的API
 *
 */
public class EventTimeTumblingWindowAllDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //老版本的API，首先要设置时间标准（时间特征）
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //[1629680400000,1629680405000)
        //[1629680400000,1629680404999]
        //1629680400000,1
        //1629680401000,2
        //1629680404000,2
        //1629680405000,5
        //1629680409998,5
        //1629680409999,5
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //提取数据中的EventTime
        //提取完EventTime生成Watermark后，对应的DataStream的数据类型和数据格式没有改变
        //1629680401000,2
        SingleOutputStreamOperator<String> streamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
            @Override
            public long extractTimestamp(String element) {
                //1629680400000,1
                return Long.parseLong(element.split(",")[0]);
            }
        });

        SingleOutputStreamOperator<Integer> nums = streamWithWaterMark.map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String line) throws Exception {
                return Integer.parseInt(line.split(",")[1]);
            }
        });

        //AllWindowedStream<Integer, TimeWindzow> window = nums.timeWindowAll(Time.seconds(5));
        AllWindowedStream<Integer, TimeWindow> window = nums.windowAll(TumblingEventTimeWindows.of(Time.seconds(5)));

        SingleOutputStreamOperator<Integer> sum = window.sum(0);

        sum.print();

        env.execute();
    }

}
