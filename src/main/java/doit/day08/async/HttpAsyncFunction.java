//package doit.day08.async;
//
//import org.apache.flink.api.java.tuple.Tuple2;
//import org.apache.flink.configuration.Configuration;
//import org.apache.flink.streaming.api.functions.async.ResultFuture;
//import org.apache.flink.streaming.api.functions.async.RichAsyncFunction;
//import org.apache.http.HttpResponse;
//import org.apache.http.client.config.RequestConfig;
//import org.apache.http.client.methods.HttpGet;
//import org.apache.http.impl.nio.client.CloseableHttpAsyncClient;
//import org.apache.http.impl.nio.client.HttpAsyncClients;
//import org.apache.http.util.EntityUtils;
//
//import java.util.Collections;
//import java.util.concurrent.CompletableFuture;
//import java.util.concurrent.Future;
//import java.util.function.Supplier;
//
//public class HttpAsyncFunction extends RichAsyncFunction<String, Tuple2<String, String>> {
//
//    private transient CloseableHttpAsyncClient httpclient; //异步请求的HttpClient
//    private String url; //请求的URL地址
//    private int maxConnTotal; //异步HTTPClient支持的最大连接
//    public HttpAsyncFunction(String url, int maxConnTotal) {
//        this.url = url;
//        this.maxConnTotal = maxConnTotal;
//    }
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        RequestConfig requestConfig = RequestConfig.custom().build();
//        httpclient = HttpAsyncClients.custom() //创建HttpAsyncClients请求连接池
//                .setMaxConnTotal(maxConnTotal) //设置最大连接数
//                .setDefaultRequestConfig(requestConfig).build();
//        httpclient.start(); //启动异步请求httpClient
//    }
//    @Override
//    public void asyncInvoke(String uid, final ResultFuture<Tuple2<String, String>> resultFuture)
//            throws Exception {
//        HttpGet httpGet = new HttpGet(url + "/?uid=" + uid); //请求的地址和参数
//        Future<HttpResponse> future = httpclient.execute(httpGet, null); //执行请求返回future
//        CompletableFuture.supplyAsync(new Supplier<String>() {
//            @Override
//            public String get() {
//                try {
//                    HttpResponse response = future.get(); //调用Future的get方法获取请求的结果
//                    String res = null;
//                    if(response.getStatusLine().getStatusCode() == 200) {
//                        res = EntityUtils.toString(response.getEntity());
//                    }
//                    return res;
//                } catch (Exception e) {
//                    return null;
//                }
//            }
//        }).thenAccept((String result) -> {
//            //将结果添加到resultFuture中输出（complete方法的参数只能为集合，如果只有一个元素，就返回一个单例集合）
//            resultFuture.complete(Collections.singleton(Tuple2.of(uid, result)));
//        });
//    }
//    @Override
//    public void close() throws Exception {
//        httpclient.close(); //关闭HttpAsyncClients请求连接池
//    }
//}