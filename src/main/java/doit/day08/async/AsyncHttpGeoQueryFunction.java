//package doit.day08.async;
//
//import com.alibaba.fastjson.JSON;
//import com.alibaba.fastjson.JSONObject;
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
//public class AsyncHttpGeoQueryFunction extends RichAsyncFunction<String, LogBean> {
//
//    private transient CloseableHttpAsyncClient httpclient; //异步请求的HttpClient
//    private String url; //请求高德地图URL地址
//    private String key; //请求高德地图的秘钥，注册高德地图开发者后获得
//    private int maxConnTotal; //异步HTTPClient支持的最大连接
//
//    public AsyncHttpGeoQueryFunction(String url, String key, int maxConnTotal) {
//        this.url = url;
//        this.key = key;
//        this.maxConnTotal = maxConnTotal;
//    }
//
//    /**
//     * 初始化异步http请求的连接池
//     * @param parameters
//     * @throws Exception
//     */
//    @Override
//    public void open(Configuration parameters) throws Exception {
//        RequestConfig requestConfig = RequestConfig.custom().build();
//        httpclient = HttpAsyncClients.custom() //创建HttpAsyncClients请求连接池
//                .setMaxConnTotal(maxConnTotal) //设置最大连接数
//                .setDefaultRequestConfig(requestConfig).build();
//        httpclient.start(); //启动异步请求httpClient
//    }
//
//    @Override
//    public void asyncInvoke(String line, ResultFuture<LogBean> resultFuture) throws Exception {
//        //使用fastjson将json字符串解析成json对象
//        LogBean bean = JSON.parseObject(line, LogBean.class);
//        double longitude = bean.longitude; //获取经度
//        double latitude = bean.latitude; //获取维度
//        //将经纬度和高德地图的key与请求的url进行拼接
//        HttpGet httpGet = new HttpGet(url + "?location=" + longitude + "," + latitude + "&key=" + key);
//        //发送异步请求，返回Future
//        Future<HttpResponse> future = httpclient.execute(httpGet, null);
//        CompletableFuture.supplyAsync(new Supplier<LogBean>() {
//            @Override
//            public LogBean get() {
//                try {
//                    HttpResponse response = future.get();
//                    String province = null;
//                    String city = null;
//                    if (response.getStatusLine().getStatusCode() == 200) {
//                        //解析返回的结果，获取省份、城市等信息
//                        String result = EntityUtils.toString(response.getEntity());
//                        JSONObject jsonObj = JSON.parseObject(result);
//                        JSONObject regeocode = jsonObj.getJSONObject("regeocode");
//                        if (regeocode != null && !regeocode.isEmpty()) {
//                            JSONObject address = regeocode.getJSONObject("addressComponent");
//                            province = address.getString("province");
//                            city = address.getString("city");
//                        }
//                    }
//                    bean.province = province; //将返回的结果给省份赋值
//                    bean.city = city; //将返回的结果给城市赋值
//                    return bean;
//                } catch (Exception e) {
//                    return null;
//                }
//            }
//        }).thenAccept((LogBean result) -> {
//            //将结果添加到resultFuture中输出（complete方法的参数只能为集合，如果只有一个元素，就返回一个单例集合）
//            resultFuture.complete(Collections.singleton(result));
//        });
//    }
//    @Override
//    public void close() throws Exception {
//        httpclient.close(); //关闭HttpAsyncClients请求连接池
//    }
//}
