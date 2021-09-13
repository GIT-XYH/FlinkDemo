package com.rookiex.day03.transformations;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * 使用KeyBy进行分区（hash partitioning）
 */
public class KeyByDemo6 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration());

        //辽宁省,沈阳市,1000
        //辽宁省,大连市,1000
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

        KeyedStream<Tuple3<String, String, Double>, ProvinceAndCity> keyed = tpDataStream.keyBy(tp -> ProvinceAndCity.of(tp.f0, tp.f1));


        SingleOutputStreamOperator<Tuple3<String, String, Double>> summed = keyed.sum(2);

        summed.print();

        env.execute();

    }


    public static class ProvinceAndCity {

        public String province;

        public String city;

        public ProvinceAndCity(){}
        public ProvinceAndCity(String province, String city) {
            this.province = province;
            this.city = city;
        }

        public static ProvinceAndCity of(String province, String city) {
            return new ProvinceAndCity(province, city);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            ProvinceAndCity that = (ProvinceAndCity) o;
            return province.equals(that.province) && city.equals(that.city);
        }

        @Override
        public int hashCode() {
            return Objects.hash(province, city);
        }
    }
}
