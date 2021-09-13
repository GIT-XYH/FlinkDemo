package doit.day08;

import com.alibaba.fastjson.JSON;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

/**
 * ProcessFunction 是Flink流式计算底层API，可以访问底层的属性和方法
 * <p>
 * 1.可以处理数
 * 2.可以使用状态（只针对于KeyedStream）
 * 3.可以使用定时器（只针对于KeyedStream）
 * <p>
 * NonKeyedStream使用ProcessFunction
 */
public class ProcessFunctionDemo1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        //{"name": "laozhao", "age": 18, "fv": 9999.99}
        //{"name": "laudan", "age": 38, "fv": 9999.9
        //{"name": "nianhang" "age": 30, "fv": 9999.9}
        DataStreamSource<String> lines = env.socketTextStream("localhost", 8888);

//        SingleOutputStreamOperator<Boy> tfBoy = lines.map(new MapFunction<String, Boy>() {
//            @Override
//            public Boy map(String line) throws Exception {
//                Boy boy = null;
//                try {
//                    boy = JSON.parseObject(line, Boy.class);
//                } catch (Exception e) {
//                    //e.printStackTrace();
//                    //将异常记录下来，并且将错误数据单独存储
//                }
//                return boy;
//            }
//        });
//
//        SingleOutputStreamOperator<Boy> filtered = tfBoy.filter(b -> b != null);


        SingleOutputStreamOperator<Boy> filtered = lines.process(new ProcessFunction<String, Boy>() {

            @Override
            public void processElement(String in, Context ctx, Collector<Boy> out) throws Exception {

                try {
                    Boy boy = JSON.parseObject(in, Boy.class);
                    out.collect(boy);
                } catch (Exception e) {
                    //e.printStackTrace();
                    //将异常记录下来，并且将错误数据单独存储
                }

            }
        });

        filtered.print();

        env.execute();

    }


    private static class Boy {

        private String name;

        private Integer age;

        private Double fv;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Integer getAge() {
            return age;
        }

        public void setAge(Integer age) {
            this.age = age;
        }

        public Double getFv() {
            return fv;
        }

        public void setFv(Double fv) {
            this.fv = fv;
        }
    }
}
