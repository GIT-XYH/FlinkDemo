package com.rookiex.day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * @Author RookieX
 * @Date 2021/8/28 8:24 下午
 * @Description:
 * NonKeyedStream 使用 ProcessFunction
 * 处理的数据
 * {"name": "老张", "age": 19, "fv": 99.99}
 * {"name": "老李", "age": 29, "fv": 929.99
 * {"name": "老刘", "age": 39, "fv": 994.99}
 */
public class ProcessFunctionDemo01 {
    public static void main(String[] args) throws Exception {
        //执行环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //数据源
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

        //处理数据
        SingleOutputStreamOperator<Object> tfBoys = lines.map(new MapFunction<String, Object>() {

            @Override
            public Object map(String line) throws Exception {
                Boy boy = null;

                try {
                    boy = JSON.parseObject(line, Boy.class);
                } catch (Exception e) {
//                    e.printStackTrace();
                    //将异常记录下来, 并将错误数据单独保存
                }
                return boy;
            }
        });

        SingleOutputStreamOperator<Object> filtered = tfBoys.filter(Objects::nonNull);
        filtered.print();
        env.execute();

    }
    private static class Boy{
        String name;
        int age;
        double fv;

        @Override
        public String toString() {
            return "Boy{" +
                    "name='" + name + '\'' +
                    ", age=" + age +
                    ", fv=" + fv +
                    '}';
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public double getFv() {
            return fv;
        }

        public void setFv(double fv) {
            this.fv = fv;
        }

        public Boy(String name, int age, double fv) {
            this.name = name;
            this.age = age;
            this.fv = fv;
        }

        public Boy() {
        }
    }
}
