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

package testframework;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Created by chuanqin on 17/9/27.
 */
public class TestCaseScanner {

    public static void scan(String packageName, List<Class<?>> list) throws Exception {
        String path = getSrcPath() + packageToDir(packageName);
        File dir = new File(path);
        File[] files = dir.listFiles();
        Class<?> cla = null;
        for (File f : files) {
            if (f.isDirectory()) {
                String childName = packageName + "." + f.getName();
                scan(childName, list);

            } else {
                cla = Class.forName(packageName + "." + f.getName().split("\\.")[0]);
                list.add(cla);
            }
        }
    }

    /**
     * 获取当前路径
     */
    public static String getSrcPath() throws IOException {
        File file = new File("");
        String path = file.getCanonicalPath() + File.separator + "polardbx-executor" + File.separator + "src"
            + File.separator + "test" + File.separator + "java";
        return path;
    }

    /**
     * package转换为路径格式
     */
    public static String packageToDir(String packageName) {
        String[] array = packageName.split("\\.");
        StringBuffer sb = new StringBuffer();
        for (String str : array) {
            sb.append(File.separator).append(str);
        }
        return sb.toString();
    }
}
