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
///**
// * 异步IO是用来查询外部的数据库、或API（REST即HTTP的api）关联维度数据
// *
// * 1.数据太多，无法缓存到内存或就不能使用BroadcastState
// * 2.跟本就没有要关联的数据，必须查别人的API
// *
// * 同步查询，来一条查一次，太慢了，会出现性能瓶颈，在一个subTask请求是串行的
// *
// * 异步查询：开多线程查询，查询快（可以在一段时间内发送更多的请求）但是消耗资源多
// *
// *
// *
// */
//
//public class AsyncQueryFromHttpDemo1 {
//
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        String url = "http://localhost:8080/api"; //异步IO发生REST请求的地址
//        int capacity = 20; //最大异步并发请求数量
//        //使用AsyncDataStream调用unorderedWait方法，并传入异步请求的Function
//        //unorderedWait请求的先后顺序可以与返回结果的顺序不一致
//        //orderedWait请求的先后顺序可以与返回结果的顺序一致
//        DataStream<Tuple2<String, String>> result = AsyncDataStream.unorderedWait(
//                lines, //输入的数据流
//                new HttpAsyncFunction(url, capacity), //异步查询的Function实例
//                5000, //超时时间
//                TimeUnit.MILLISECONDS, //时间单位
//                capacity); //异步请求队列最大的数量，不传该参数默认值为100
//        result.print();
//        env.execute();
//    }
//}
