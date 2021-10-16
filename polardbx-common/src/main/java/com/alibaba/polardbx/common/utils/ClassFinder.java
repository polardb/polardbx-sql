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

import com.google.common.collect.Lists;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;


public class ClassFinder {

    public static List<Class> findClassesInPackage(String packageName, ClassFilter filter) {
        try {
            List<String> classes = Lists.newArrayList();
            String packageDirName = packageName.replace('.', '/');
            ClassLoader classLoader = ClassFinder.class.getClassLoader();

            Enumeration<URL> dirEnumeration = classLoader.getResources(packageDirName);
            List<URL> dirs = Lists.newArrayList();
            while (dirEnumeration.hasMoreElements()) {
                URL dir = dirEnumeration.nextElement();
                dirs.add(dir);
            }

            Iterator<URL> dirIterator = dirs.iterator();
            while (dirIterator.hasNext()) {
                URL url = dirIterator.next();
                String protocol = url.getProtocol();
                if ("file".equals(protocol)) {
                    findClassesInDirPackage(packageName, URLDecoder.decode(url.getFile(), "UTF-8"), classes);
                } else if ("jar".equals(protocol)) {
                    JarFile jar = ((JarURLConnection) url.openConnection()).getJarFile();
                    Enumeration<JarEntry> entries = jar.entries();
                    while (entries.hasMoreElements()) {
                        JarEntry entry = entries.nextElement();
                        String name = entry.getName();
                        String pName = packageName;
                        if (name.charAt(0) == '/') {
                            name = name.substring(1);
                        }
                        if (name.startsWith(packageDirName)) {
                            int idx = name.lastIndexOf('/');
                            if (idx != -1) {
                                pName = name.substring(0, idx).replace('/', '.');
                            }

                            if (idx != -1) {

                                if (name.endsWith(".class") && !entry.isDirectory()) {
                                    String className = name.substring(pName.length() + 1, name.length() - 6);
                                    classes.add(makeFullClassName(pName, className));
                                }
                            }
                        }
                    }
                }
            }

            List<Class> result = Lists.newArrayList();
            for (String clazz : classes) {
                if (filter == null || filter.preFilter(clazz)) {
                    Class<?> cls = null;
                    try {
                        cls = Class.forName(clazz);
                    } catch (Throwable e) {

                    }

                    if (cls != null && (filter == null || filter.filter(cls))) {
                        result.add(cls);
                    }
                }
            }

            return result;
        } catch (IOException e) {
            throw GeneralUtil.nestedException("findClassesInPackage : " + packageName + " is failed. ", e);
        }

    }

    public static List<Class> getAllClassByInterface(Class c, String packageName) {
        List<Class> returnClassList = new ArrayList<Class>();
        if (c.isInterface()) {

            try {
                List<Class> allClass = findClassesInPackage(packageName, null);

                for (int i = 0; i < allClass.size(); i++) {
                    if (c.isAssignableFrom(allClass.get(i))) {

                        if (!c.equals(allClass.get(i))) {

                            returnClassList.add(allClass.get(i));
                        }
                    }
                }
            } catch (Exception e) {

            }

        }

        return returnClassList;
    }

    private static void findClassesInDirPackage(String packageName, String packagePath, List<String> classes) {
        File dir = new File(packagePath);
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }

        File[] dirfiles = dir.listFiles(new FileFilter() {

            @Override
            public boolean accept(File file) {
                return file.isDirectory() || (file.getName().endsWith(".class"));
            }
        });

        for (File file : dirfiles) {
            if (file.isDirectory()) {
                findClassesInDirPackage(makeFullClassName(packageName, file.getName()), file.getAbsolutePath(),
                    classes);
            } else {
                String className = file.getName().substring(0, file.getName().lastIndexOf("."));
                classes.add(makeFullClassName(packageName, className));
            }
        }
    }

    private static String makeFullClassName(String pkg, String cls) {
        return pkg.length() > 0 ? pkg + "." + cls : cls;
    }

    public interface ClassFilter {

        public boolean preFilter(String className);

        public boolean filter(Class clazz);
    }
}
