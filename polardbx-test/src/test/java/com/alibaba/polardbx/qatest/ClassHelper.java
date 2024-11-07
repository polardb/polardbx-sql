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

package com.alibaba.polardbx.qatest;

import com.alibaba.polardbx.common.utils.ClassFinder;
import com.alibaba.polardbx.qatest.ddl.auto.columnar.CreateCciTest;
import com.google.common.collect.ImmutableList;

import java.lang.reflect.Modifier;
import java.util.List;
import java.util.stream.Collectors;

public class ClassHelper {
    public static final String PACKAGE_NAME = "com.alibaba.polardbx.qatest";

    public static ImmutableList<Class> fileStorageTestCases;

    public static ImmutableList<Class> columnarTestCases;

    static {
        // Scan all class in this package and register the unit case on file storage.
        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
            @Override
            public boolean filter(Class klass) {
                int mod = klass.getModifiers();
                return !Modifier.isAbstract(mod)
                    && !Modifier.isInterface(mod)
                    && ReadBaseTestCase.class.isAssignableFrom(klass)
                    // supper class of AutoReadBaseTestCase, or has TestFileStorage annotation.
                    && (AutoReadBaseTestCase.class.isAssignableFrom(klass)
                    || klass.getAnnotation(TestFileStorage.class) != null);
            }

            @Override
            public boolean preFilter(String classFulName) {
                return classFulName.endsWith("Test");
            }
        };

        List<Class> filtered = ClassFinder.findClassesInPackage(PACKAGE_NAME, filter);
        fileStorageTestCases = ImmutableList.<Class>builder().addAll(filtered).build();
    }

    public static ImmutableList<Class> getFileStorageTestCases() {
        return fileStorageTestCases;
    }

    public static ImmutableList<Class> getColumnarTestCases() {
        if (columnarTestCases == null) {
            synchronized (ClassHelper.class) {
                if (columnarTestCases == null) {
                    columnarTestCases = ImmutableList.<Class>builder().addAll(fileStorageTestCases.stream()
                        .filter(klass -> klass.getAnnotation(TestFileStorage.class) == null)
                        .collect(Collectors.toList()))
                        .addAll(getColumnarDqlCase())
                        .add(CreateCciTest.class)
                        .build();
                }
            }
        }
        return columnarTestCases;
    }

    private static List<Class> getColumnarDqlCase() {
        String columnarPackageName = "com.alibaba.polardbx.qatest.columnar.dql";
        ClassFinder.ClassFilter filter = new ClassFinder.ClassFilter() {
            @Override
            public boolean filter(Class klass) {
                int mod = klass.getModifiers();
                return !Modifier.isAbstract(mod)
                    && !Modifier.isInterface(mod);
            }

            @Override
            public boolean preFilter(String classFulName) {
                return classFulName.endsWith("Test");
            }
        };
        return ClassFinder.findClassesInPackage(columnarPackageName, filter);
    }
}
