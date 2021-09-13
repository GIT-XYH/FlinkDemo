package com.rookiex.day04;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * project（动词：投影、隐射）
 *
 */
public class ProjectDemo {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //辽宁省,沈阳市,1000
        //辽宁省,大连市,1000
        //辽宁省,本溪市,2000
        //山东省,济南市,1000
        //山东省,烟台市,1000
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        SingleOutputStreamOperator<Tuple3<String, String, Double>> tpDataStream = lines.map(new MapFunction<String, Tuple3<String, String, Double>>() {
            @Override
            public Tuple3<String, String, Double> map(String line) throws Exception {
                String[] fields = line.split(",");
                return Tuple3.of(fields[0], fields[1], Double.parseDouble(fields[2]));
            }
        });

        //按照省份进行KeyBy
        KeyedStream<Tuple3<String, String, Double>, String> keyed = tpDataStream.keyBy(t -> t.f0);


        SingleOutputStreamOperator<Tuple3<String, String, Double>> max = keyed.max(2);

        //做隐射
        //SingleOutputStreamOperator<Tuple2<String, Double>> res = max.map(t -> Tuple2.of(t.f0, t.f2)).returns(Types.TUPLE(Types.STRING, Types.INT));

        //project方法只能针对于Tuple类型的DataStream
        SingleOutputStreamOperator<Tuple> res = max.project(2, 0);

        res.print();

        env.execute();


    }


}
