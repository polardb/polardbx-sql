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

package com.alibaba.polardbx.druid;


import com.alibaba.polardbx.druid.util.JdbcUtils;
import org.apache.commons.io.IOUtils;

import java.io.InputStream;
import java.net.URL;

public class ResourceUtils {
    public static String load(String path) throws Exception {
        InputStream is = null;
        try {
            is = Thread.currentThread()
                    .getContextClassLoader()
                    .getResourceAsStream(path);
            return IOUtils.toString(is);
        } finally {
            JdbcUtils.close(is);
        }
    }
     public static URL loadResource(String path) throws Exception {
         return Thread.currentThread()
                 .getContextClassLoader()
                 .getResource(path);
    }

}
