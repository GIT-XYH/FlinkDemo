//package doit.day08.async;
//
//import org.apache.flink.api.common.restartstrategy.RestartStrategies;
//import org.apache.flink.streaming.api.datastream.AsyncDataStream;
//import org.apache.flink.streaming.api.datastream.DataStreamSource;
//import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
//import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
//
//import java.util.concurrent.TimeUnit;
//
///**
// * 异步IO是用来查询外部的数据库、或API（REST即HTTP的api）关联维度数据
// * 1.数据太多，无法缓存到内存或使用BroadcastState
// * 2.跟本就没有要关联的数据，必须查别人的API
// *
// * 同步查询，来一条查一次，太慢了，会出现性能瓶颈，在一个subTask请求是串行的
// *
// * 异步查询：开多线程查询，查询快（可以在一段时间内发生更多的请求）但是消耗资源多
// *
// *  根据经纬度，请求高德地图，返回省、市、区等信息
// */
//
//
//public class AsyncQueryFromHttpDemo2 {
//
//    public static void main(String[] args) throws Exception {
//
//        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
//        //设置job的重启策略
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000));
//        /**
//         * {"oid":"o124", "cid": 2, "money": 200.0, "longitude":117.397128,"latitude":38.916527}
//         * {"oid":"o125", "cid": 3, "money": 100.0, "longitude":118.397128,"latitude":35.916527}
//         * {"oid":"o127", "cid": 1, "money": 100.0, "longitude":116.395128,"latitude":39.916527}
//         * {"oid":"o128", "cid": 2, "money": 200.0, "longitude":117.396128,"latitude":38.916527}
//         * {"oid":"o129", "cid": 3, "money": 300.0, "longitude":115.398128,"latitude":35.916527}
//         * {"oid":"o130", "cid": 2, "money": 100.0, "longitude":116.397128,"latitude":39.916527}
//         * {"oid":"o131", "cid": 1, "money": 100.0, "longitude":117.394128,"latitude":38.916527}
//         * {"oid":"o132", "cid": 3, "money": 200.0, "longitude":118.396128,"latitude":35.916527}
//         */
//        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);
//        String url = "https://restapi.amap.com/v3/geocode/regeo"; //异步请求高德地图的地址
//        String key = "4924f7ef5c86a278f5500851541cdcff"; //请求高德地图的秘钥，注册高德地图开发者后获得
//        int capacity = 50; //最大异步并发请求数量
//        //使用AsyncDataStream调用unorderedWait方法，并传入异步请求的Function
//
//
//        //unorderedWait 发送的请求和响应的结果是没有顺序的
//        //orderedWait 发送的请求和响应的结果是有顺序的，先请求先返回
//        SingleOutputStreamOperator<LogBean> result = AsyncDataStream.unorderedWait(
//                lines, //输入的数据流
//                new AsyncHttpGeoQueryFunction(url, key, capacity), //异步查询的Function实例
//                3000, //超时时间
//                TimeUnit.MILLISECONDS, //时间单位
//                capacity);//异步请求队列最大的数量，不传该参数默认值为100
//        result.print();
//        env.execute();
//    }
//}
