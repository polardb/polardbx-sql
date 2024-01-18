/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.utils;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

public class HttpClientHelper {

    private static final int READ_TIMEOUT = 50000;
    private static final int CONNECT_TIMEOUT = 20000;
    public static final int HTTP_OK = 200;
    public static final String DEFAULT_ENCODING = "utf-8";

    public static String doGet(String url) {
        return doGet(url, null);
    }

    public static String doGet(String url, Map<String, String> params) {
        return doGet(url, params, null);
    }

    public static String doGet(String url, Map<String, String> params, Map<String, String> headers) {
        try {
            String paramStr = encodingParams(params, DEFAULT_ENCODING);
            String fullUrl = (paramStr == null) ? url : url + "?" + paramStr;

            URL u = new URL(fullUrl);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            addHeaders(conn, headers);
            conn.setRequestMethod("GET");
            conn.setReadTimeout(READ_TIMEOUT);
            conn.setConnectTimeout(CONNECT_TIMEOUT);
            conn.connect();
            int i = conn.getResponseCode();
            if (i == HttpURLConnection.HTTP_OK) {
                return IOUtils.toString(conn.getInputStream(), DEFAULT_ENCODING);
            } else {
                InputStream is = conn.getErrorStream();
                String errorMsg = "response not ok,http status:" + i + ",url:" + url + ",params:" + params;
                if (is != null) {
                    errorMsg += ",responsed error msg is:" + IOUtils.toString(is, DEFAULT_ENCODING);
                }
                throw new RuntimeException(errorMsg);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("url format error.the url is:" + url, e);
        } catch (IOException e) {
            throw new RuntimeException("connect to " + url + " error.", e);
        }
    }

    public static String doPost(String url) {
        return doPost(url, null);
    }

    public static String doPost(String url, String paramStr) {
        return doPost(url, paramStr, null, null);
    }

    public static String doPostMethod(String url, String method) {
        return doPost(url, null, null, method);
    }

    public static String doPost(String url, String paramStr, String method) {
        return doPost(url, paramStr, null, method);
    }

    public static String doPost(String url, String paramStr, Map<String, String> headers, String method) {
        try {
            URL u = new URL(url);
            HttpURLConnection conn = (HttpURLConnection) u.openConnection();
            addHeaders(conn, headers);
            if (StringUtils.isBlank(method)) {
                conn.setRequestMethod("POST");
            } else {
                conn.setRequestMethod(method);
            }

            conn.setDoInput(true);
            conn.setDoOutput(true);
            conn.setReadTimeout(READ_TIMEOUT);
            conn.setConnectTimeout(CONNECT_TIMEOUT);
            conn.connect();

            if (StringUtils.isNotBlank(paramStr)) {
                conn.getOutputStream().write(paramStr.getBytes());
                conn.getOutputStream().flush();
            }

            int i = conn.getResponseCode();
            if (i == HttpURLConnection.HTTP_OK) {
                return IOUtils.toString(conn.getInputStream(), DEFAULT_ENCODING);
            } else {
                InputStream is = conn.getErrorStream();
                String errorMsg = "response not ok,http status:" + i + ",url:" + url + ",params:"
                    + (paramStr == null ? "" : paramStr);
                if (is != null) {
                    errorMsg += ",responsed error msg is:" + IOUtils.toString(is, DEFAULT_ENCODING);
                }
                throw new RuntimeException(errorMsg);
            }
        } catch (MalformedURLException e) {
            throw new IllegalArgumentException("url format error.the url is:" + url, e);
        } catch (IOException e) {
            throw new RuntimeException("connect to " + url + " error.", e);
        }
    }

    private static void addHeaders(HttpURLConnection conn, Map<String, String> headers) {
        if (headers != null) {
            for (Map.Entry<String, String> h : headers.entrySet()) {
                conn.addRequestProperty(h.getKey(), h.getValue());
            }
        }
    }

    public static String encodingParams(Map<String, String> params, String encoding)
        throws UnsupportedEncodingException {
        if (params == null || params.size() == 0) {
            return null;
        }

        StringBuilder sb = new StringBuilder();
        Iterator<String> keys = params.keySet().iterator();
        for (; keys.hasNext(); ) {
            String key = keys.next();
            sb.append(key).append("=").append(URLEncoder.encode(params.get(key), encoding));
            if (keys.hasNext()) {
                sb.append("&");
            }
        }

        return sb.toString();
    }
}
