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

package com.alibaba.polardbx.atom.common;

import com.alibaba.polardbx.common.utils.TStringUtil;
import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;
import java.util.Map;

/**
 * 数据库连接URL生成工具类
 *
 * @author qihao
 */
public class TAtomConURLTools {

    private static final MessageFormat MYSQL_URL_FORMAT = new MessageFormat("jdbc:mysql://{0}:{1}/{2}");

    public static String getMySqlConURL(String ip, String port, String dbName, Map<String, String> prams) {
        String conUrl = null;
        if (checkPrams(ip, port, dbName)) {
            conUrl = MYSQL_URL_FORMAT.format(new String[] {ip, port, dbName});
            StringBuilder sb = new StringBuilder();
            if (prams != null) {
                for (Map.Entry<String, String> entry : prams.entrySet()) {
                    String key = entry.getKey();
                    String value = entry.getValue();
                    if (TStringUtil.isNotBlank(key) && TStringUtil.isNotBlank(value)) {
                        sb.append(key);
                        sb.append("=");
                        sb.append(value);
                        sb.append("&");
                    }
                }
            }
            String pramStr = TStringUtil.substringBeforeLast(sb.toString(), "&");
            if (StringUtils.isNotEmpty(pramStr)) {
                conUrl = conUrl + "?" + pramStr;
            }
        }
        return conUrl;
    }

    private static boolean checkPrams(String ip, String port, String dbName) {
        boolean flag = TStringUtil.isNotBlank(ip) && TStringUtil.isNotBlank(port) && TStringUtil.isNotBlank(dbName);
        return flag;
    }
}
