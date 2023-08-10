package com.zyd.common.utils;

import com.alibaba.fastjson.JSON;
import com.zyd.common.constance.Constants;
import okhttp3.*;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.X509TrustManager;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.collections4.CollectionUtils;

/**
 * @author 85728
 */
public class OkHttpUtils {

    private static final Logger log = LoggerFactory.getLogger(OkHttpUtils.class);

    private static OkHttpClient okHttpClient;
    private static Semaphore semaphore = null;
    private Map<String, String> headerMap;
    private Map<String, String> paramMap;
    private Map<String, String> variablesMap;
    private Map<String, Collection> multipleValueParamMap;
    private String url;
    private Request.Builder request;

    static {
        TrustManager[] trustManagers = buildTrustManagers();
        okHttpClient = new OkHttpClient.Builder()
                .connectTimeout(30, TimeUnit.SECONDS)
                .writeTimeout(60, TimeUnit.SECONDS)
                .readTimeout(60, TimeUnit.SECONDS)
                .sslSocketFactory(createSSLSocketFactory(trustManagers), (X509TrustManager) trustManagers[0])
                .hostnameVerifier((hostName, session) -> hostName.equalsIgnoreCase(session.getPeerHost()))
                .retryOnConnectionFailure(false)
                .build();
    }

    /**
     * 初始化okHttpClient，并且允许https访问
     */
    private OkHttpUtils() {
        addHeader("User-Agent", "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/63.0.3239.132 Safari/537.36");
    }

    /**
     * 用于异步请求时，控制访问线程数，返回结果
     *
     * @return
     */
    private static Semaphore getSemaphoreInstance() {
        //只能1个线程同时访问
        synchronized (OkHttpUtils.class) {
            if (semaphore == null) {
                semaphore = new Semaphore(0);
            }
        }
        return semaphore;
    }

    /**
     * 创建OkHttpUtils
     *
     * @return
     */
    public static OkHttpUtils builder() {
        return new OkHttpUtils();
    }

    public OkHttpClient getHttpClient(){
        return okHttpClient;
    }
    /**
     * 添加url
     *
     * @param url
     * @return
     */
    public OkHttpUtils url(String url) {
        this.url = url;
        return this;
    }

    /**
     * 添加参数
     *
     * @param key   参数名
     * @param value 参数值
     * @return
     */
    public OkHttpUtils addParam(String key, String value) {
        if (paramMap == null) {
            paramMap = new LinkedHashMap<>(16);
        }
        paramMap.put(key, value);
        return this;
    }

    /**
     * 添加参数
     * @param key   参数名
     * @param value 参数值
     * @return
     */
    public OkHttpUtils addVariables(String key, String value) {
        if (variablesMap == null) {
            variablesMap = new LinkedHashMap<>();
        }
        variablesMap.put(key, value);
        return this;
    }

    /**
     * 添加多值参数
     *
     * @param key   参数名
     * @param value 参数值
     * @return
     */
    public OkHttpUtils addParam(String key, Collection value) {
        if (paramMap == null) {
            multipleValueParamMap = new HashMap<>();
        }
        multipleValueParamMap.put(key, value);
        return this;
    }

    /**
     * 添加请求头
     *
     * @param key   参数名
     * @param value 参数值
     * @return
     */
    public OkHttpUtils addHeader(String key, String value) {
        if (headerMap == null) {
            headerMap = new LinkedHashMap<>(16);
        }
        headerMap.put(key, value);
        return this;
    }


    /**
     * 初始化get方法
     *
     * @return
     */
    public OkHttpUtils get() {
        request = new Request.Builder().get();
        StringBuilder urlBuilder = new StringBuilder(url);
        if (paramMap != null || multipleValueParamMap != null) {
            urlBuilder.append("?");
        }
        if (paramMap != null) {
            try {
                for (Map.Entry<String, String> entry : paramMap.entrySet()) {
                    urlBuilder.append(URLEncoder.encode(entry.getKey(), "utf-8")).
                            append("=").
                            append(URLEncoder.encode(entry.getValue(), "utf-8")).
                            append("&");
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
        if (multipleValueParamMap != null) {
            multipleValueParamMap.forEach((k, v) -> {
                v.forEach(singleValue -> {
                    try {
                        urlBuilder.append(URLEncoder.encode(k, "utf-8")).
                                append("=").
                                append(URLEncoder.encode(singleValue.toString(), "utf-8")).
                                append("&");
                    } catch (UnsupportedEncodingException e) {
                        log.error(e.getMessage());
                    }
                });

            });
        }
        if (paramMap != null || multipleValueParamMap != null) {
            urlBuilder.deleteCharAt(urlBuilder.length() - 1);
        }
        request.url(urlBuilder.toString());
        return this;
    }

    /**
     * 初始化post方法
     *
     * @param isJsonPost true等于json的方式提交数据，类似postman里post方法的raw
     *                   false等于普通的表单提交
     * @return
     */
    public OkHttpUtils post(boolean isJsonPost) {
        RequestBody requestBody;
        if (isJsonPost) {
            String json = "";
            if (paramMap != null) {
                json = JSON.toJSONString(paramMap);
            }
            requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);
        } else {
            FormBody.Builder formBody = new FormBody.Builder();
            if (paramMap != null) {
                paramMap.forEach(formBody::add);
            }
            requestBody = formBody.build();
        }
        request = new Request.Builder().post(requestBody).url(url);
        return this;
    }

    /**
     * 初始化post方法
     *
     * @param json true等于json的方式提交数据，类似postman里post方法的raw
     *             false等于普通的表单提交
     * @return
     */
    public OkHttpUtils postByJson(String json) {
        RequestBody requestBody;
        requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);
        if (MapUtils.isNotEmpty(paramMap)) {
            paramMap.entrySet().stream().map(e -> String.format("%s=%s", e.getKey(), e.getValue())).collect(Collectors.joining(Constants.AMPERSAND));
        }
        request = new Request.Builder().post(requestBody).url(url);
        return this;
    }

    /* 
    * @Description: post请求，添加查询参数和请求体
     * @Param: [json]
    * @return: tech.zj.quanbu.daas.base.service.utils.OkHttpUtils 
    * @Author: zhongyuandong
    * @Date: 2023-08-01 16:05
    */
    public OkHttpUtils post(String json) {
        RequestBody requestBody;
        requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

        String postUrl = url;
        if (MapUtils.isNotEmpty(paramMap)) {
            String param = paramMap.entrySet().stream()
                    .filter(e -> !StringUtils.isAnyBlank(e.getKey(), e.getValue()))
                    .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(Constants.AMPERSAND));
            if (StringUtils.isNotBlank(param)){
                postUrl = String.format("%s?%s", url, param);
            }
        }
        log.info("执行post请求，地址：{} 参数：{}", postUrl, requestBody);
        request = new Request.Builder().post(requestBody).url(postUrl);
        return this;
    }

    public OkHttpUtils patch(String json) {
        RequestBody requestBody;
        requestBody = RequestBody.create(MediaType.parse("application/json; charset=utf-8"), json);

        String patchUrl = url;
        if (MapUtils.isNotEmpty(variablesMap)){
            String finalPatchUrl = url;
            variablesMap.forEach((key, value) -> {
                url = finalPatchUrl.replace(String.format(":%s", key), value);
            });
            patchUrl = url;
        }
        if (MapUtils.isNotEmpty(paramMap)) {
            String param = paramMap.entrySet().stream()
                    .filter(e -> !StringUtils.isAnyBlank(e.getKey(), e.getValue()))
                    .map(e -> String.format("%s=%s", e.getKey(), e.getValue()))
                    .collect(Collectors.joining(Constants.AMPERSAND));
            if (StringUtils.isNotBlank(param)){
                patchUrl = String.format("%s?%s", url, param);
            }
        }
        log.info("执行patch请求，地址：{} 参数：{}", patchUrl, requestBody);
        request = new Request.Builder().patch(requestBody).url(patchUrl);
        return this;
    }

    /**
     * 同步请求
     *
     * @return
     */
    public String sync() {
        setHeader(request);
        try {
            Response response = okHttpClient.newCall(request.build()).execute();
            assert response.body() != null;
            String responseString = response.body().string();
            log.debug("返回数据{}", responseString);
            return responseString;
        } catch (IOException e) {
            log.error(e.getMessage());
            return "请求失败：" + e.getMessage();
        } catch (RuntimeException runtimeException) {
            throw new RuntimeException();
        }
    }

    /**
     * 异步请求，有返回值
     */
    public String async() {
        StringBuilder buffer = new StringBuilder("");
        setHeader(request);
        okHttpClient.newCall(request.build()).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                buffer.append("请求出错：").append(e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                assert response.body() != null;
                buffer.append(response.body().string());
                getSemaphoreInstance().release();
            }
        });
        try {
            getSemaphoreInstance().acquire();
        } catch (InterruptedException e) {
            log.info(e.getMessage());
            Thread.currentThread().interrupt();
        }
        return buffer.toString();
    }

    /**
     * 异步请求，带有接口回调
     *
     * @param callBack
     */
    public void async(ICallBack callBack) {
        setHeader(request);
        okHttpClient.newCall(request.build()).enqueue(new Callback() {
            @Override
            public void onFailure(Call call, IOException e) {
                callBack.onFailure(call, e.getMessage());
            }

            @Override
            public void onResponse(Call call, Response response) throws IOException {
                assert response.body() != null;
                callBack.onSuccessful(call, response.body().string());
            }
        });
    }

    /**
     * 为request添加请求头
     *
     * @param request
     */
    private void setHeader(Request.Builder request) {
        if (headerMap != null) {
            try {
                for (Map.Entry<String, String> entry : headerMap.entrySet()) {
                    request.addHeader(entry.getKey(), entry.getValue());
                }
            } catch (Exception e) {
                log.error(e.getMessage());
            }
        }
    }


    /**
     * 生成安全套接字工厂，用于https请求的证书跳过
     *
     * @return
     */
    private static SSLSocketFactory createSSLSocketFactory(TrustManager[] trustAllCerts) {
        SSLSocketFactory ssfFactory = null;
        try {
//            SSLContext sc = SSLContext.getInstance("SSL");
            SSLContext sc = SSLContext.getInstance("TLSv1.2");
            sc.init(null, trustAllCerts, new SecureRandom());
            ssfFactory = sc.getSocketFactory();
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return ssfFactory;
    }

    private static TrustManager[] buildTrustManagers() {
        return new TrustManager[]{
                new X509TrustManager() {
                    @Override
                    public void checkClientTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                        if (false) {
                            throw new CertificateException();
                        }
                    }

                    @Override
                    public void checkServerTrusted(X509Certificate[] chain, String authType) throws CertificateException {
                        if (false) {
                            throw new CertificateException();
                        }
                    }

                    @Override
                    public X509Certificate[] getAcceptedIssuers() {
                        return new X509Certificate[]{};
                    }
                }
        };
    }

    /**
     * 自定义一个接口回调
     */
    public interface ICallBack {

        void onSuccessful(Call call, String data);

        void onFailure(Call call, String errorMsg);

    }
}
