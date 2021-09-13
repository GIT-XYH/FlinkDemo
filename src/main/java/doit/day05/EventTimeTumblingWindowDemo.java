package doit.day05;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

/**
 * 先KeyBy按照EventTime划分滚动窗口
 * Keyed Window（并行度可以是多个）
 */
public class EventTimeTumblingWindowDemo {

    public static void main(String[] args) throws Exception {


        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        //1000,spark,5
        //2000,spark,7
        //3000,hive,3
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
        //提取EventTime生成WaterMark
        SingleOutputStreamOperator<String> streamWithWaterMark = lines.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(2)) { //窗口延迟触发时间 2000 毫秒
            @Override
            public long extractTimestamp(String element) {
                //1000,spark,5
                return Long.parseLong(element.split(",")[0]);
            }
        });

        //对带水位线的数据流进行处理
        SingleOutputStreamOperator<Tuple2<String, Integer>> wordAndCount = streamWithWaterMark.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String line) throws Exception {
                //1000,spark,5
                String[] fields = line.split(",");
                return Tuple2.of(fields[1], Integer.parseInt(fields[2]));
            }
        });

        //先keyBy
        KeyedStream<Tuple2<String, Integer>, String> keyed = wordAndCount.keyBy(t -> t.f0);
        //在划分窗口
        WindowedStream<Tuple2<String, Integer>, String, TimeWindow> window = keyed.window(TumblingEventTimeWindows.of(Time.seconds(5)));

        window.sum(1).print();

        env.execute();
    }

}
