//package doit.day08.async;
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.concurrent.TimeUnit;
//
//public class AsyncQueryFromMySQL {
//
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000)); //设置job的重启策略
//        //调用Source创建DataStream
//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        int capacity = 50;
//        //AsyncDataStream.orderedWait 响应的结果的顺序和请求的先后顺序是一致的
//        DataStream<Tuple2<String, String>> result = AsyncDataStream.orderedWait(
//                lines, //输入的数据流
//                new MySQLAsyncFunction(capacity), //异步查询的Function实例
//                3000, //超时时间
//                TimeUnit.MILLISECONDS, //时间单位
//                capacity); //异步请求队列最大的数量，不传该参数默认值为100
//        result.print();
//        env.execute();
//
//    }
//}
