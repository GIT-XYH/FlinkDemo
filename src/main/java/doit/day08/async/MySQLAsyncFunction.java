//package doit.day08.async;
//
//import com.alibaba.druid.pool.DruidDataSource;
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//
//import java.sql.Connection;
//import java.sql.PreparedStatement;
//import java.sql.ResultSet;
//import java.sql.SQLException;
//import java.util.Collections;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.ExecutorService;
//import java.util.concurrent.Executors;
//import java.util.concurrent.Future;
//import java.util.function.Supplier;
//
//public class MySQLAsyncFunction extends RichAsyncFunction<String, Tuple2<String, String>> {
//
//    private transient DruidDataSource dataSource; //使用alibaba的Druid数据库连接池
//    private transient ExecutorService executorService; //用于提交多个异步请求的线程池
//
//    private int maxConnTotal; //线程池最大线程数量
//
//    public MySQLAsyncFunction(int maxConnTotal) {
//        this.maxConnTotal = maxConnTotal;
//    }
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        //创建固定的大小的线程池
//        executorService = Executors.newFixedThreadPool(maxConnTotal);
//        //创建数据库连接池并指定对应的参数
//        dataSource = new DruidDataSource();
//        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
//        dataSource.setUsername("root");
//        dataSource.setPassword("123456");
//        dataSource.setUrl("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8");
//        dataSource.setMaxActive(maxConnTotal);
//    }
//    @Override
//    public void close() throws Exception {
//        dataSource.close(); //关闭数据库连接池
//        executorService.shutdown(); //关闭线程池
//    }
//    @Override
//    public void asyncInvoke(String id, ResultFuture<Tuple2<String, String>> resultFuture) throws Exception {
//        //调用线程池的submit方法，将查询请求丢入到线程池中异步执行，返回Future对象
//        Future<String> future = executorService.submit(() -> {
//            return queryFromMySql(id); //查询数据库的方法
//        });
//        CompletableFuture.supplyAsync(new Supplier<String>() {
//            @Override
//            public String get() {
//                try {
//                    return future.get(); //获取查询的结果
//                } catch (Exception e) {
//                    return null;
//                }
//            }
//        }).thenAccept((String result) -> {
//            //将id和查询的结果用Tuple2封装，放入到ResultFuture中输出
//            resultFuture.complete(Collections.singleton(Tuple2.of(id, result)));
//        });
//    }
//
//    private String queryFromMySql(String param) throws SQLException {
//        String sql = "SELECT id, info FROM t_data WHERE id = ?";
//        String result = null;
//        Connection connection = null;
//        PreparedStatement stmt = null;
//        ResultSet rs = null;
//        try {
//            connection = dataSource.getConnection();
//            stmt = connection.prepareStatement(sql);
//            stmt.setString(1, param); //设置查询参数
//            rs = stmt.executeQuery(); //执行查询
//            while (rs.next()) {
//                result = rs.getString("info"); //返回查询结果
//            }
//        } finally {
//            if (rs != null) {
//                rs.close();
//            }
//            if (stmt != null) {
//                stmt.close();
//            }
//            if (connection != null) {
//                connection.close();
//            }
//        }
//        return result;
//    }
//}