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

package com.alibaba.polardbx.common.utils.jar;

import org.apache.commons.lang.StringUtils;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

public class JarGenerator {
    private String classpath;

    private String targetPath;
    private String jarName;

    public JarGenerator(String classpath, String targetPath, String jarName) {
        this.classpath = classpath;
        this.jarName = jarName;
        if (!this.jarName.endsWith(".jar")) {
            this.jarName += ".jar";
        }
        this.targetPath = targetPath;
    }

    public void generate() throws IOException {
        File file = new File(targetPath);
        mkDir(file);
        Manifest manifest = new Manifest();
        manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
        manifest.getMainAttributes().putValue("Archiver-Version", "Plexus Archiver");
        manifest.getMainAttributes().putValue("Built-By", "hongxi.chx");
        JarOutputStream target = new JarOutputStream(new FileOutputStream(targetPath + "/" + jarName), manifest);
        writeClassFile(new File(classpath), target);
        target.close();
    }

    public String getPath() {
        return targetPath + "/" + jarName;
    }

    private void writeClassFile(File source, JarOutputStream target) throws IOException {
        BufferedInputStream in = null;
        try {
            if (source.isDirectory()) {
                String name = source.getPath().replace("\\", "/");
                if (!StringUtils.isEmpty(name)) {
                    if (!name.endsWith("/")) {
                        name += "/";
                    }
                    name = name.substring(classpath.length() + 1);
                    if (name.length() > 0) {
                        JarEntry entry = new JarEntry(name);
                        entry.setTime(source.lastModified());
                        target.putNextEntry(entry);
                        target.closeEntry();
                    }
                }
                for (File nestedFile : source.listFiles()) {
                    writeClassFile(nestedFile, target);
                }
                return;
            }

            String middleName = source.getPath().replace("\\", "/").substring(classpath.length() + 1);
            JarEntry entry = new JarEntry(middleName);
            entry.setMethod(JarOutputStream.DEFLATED);
            entry.setTime(source.lastModified());
            target.putNextEntry(entry);
            in = new BufferedInputStream(new FileInputStream(source));

            byte[] buffer = new byte[1024];
            while (true) {
                int count = in.read(buffer);
                if (count == -1) {
                    break;
                }
                target.write(buffer, 0, count);
            }
            target.closeEntry();
        } finally {
            if (in != null) {
                in.close();
            }
        }
    }

    private void mkDir(File file) {
        if (file.getParentFile().exists()) {
            file.mkdir();
        } else {
            mkDir(file.getParentFile());
            file.mkdir();
        }
    }

}
