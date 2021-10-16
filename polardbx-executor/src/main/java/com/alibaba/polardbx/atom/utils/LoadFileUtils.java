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

package com.alibaba.polardbx.atom.utils;

import com.alibaba.druid.pool.DruidPooledStatement;
import com.alibaba.druid.proxy.jdbc.StatementProxy;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IStatement;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import java.io.InputStream;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.Callable;

public class LoadFileUtils {

    private static final Cache<Class, Method> setMethodCaches = CacheBuilder.newBuilder().build();
    private static final Cache<Class, Method> getMethodCaches = CacheBuilder.newBuilder().build();

    @SuppressWarnings("resource")
    public static void setLocalInfileInputStream(Statement stmt, InputStream stream) {
        if (stmt instanceof IStatement) {
            ((IStatement) stmt).setLocalInfileInputStream(stream);
            return;
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();
            setLocalInfileInputStream(stmt, stream);
            return;
        }

        if (stmt instanceof StatementProxy) {
            try {
                stmt = stmt.unwrap(Statement.class);
                setLocalInfileInputStream(stmt, stream);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
            }
        }

        try {
            Statement stmtnew = stmt.unwrap(Statement.class);
            if (!stmtnew.getClass().equals(stmt.getClass())) {
                setLocalInfileInputStream(stmtnew, stream);
                return;
            } else {
                stmt = stmtnew;
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        }
        try {
            final Class<?> clazz = stmt.getClass();
            Method setMethod = setMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("setLocalInfileInputStream",
                                InputStream.class);
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {

                        }

                        c = c.getSuperclass();

                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "get setLocalInfileInputStream error, clazz:" + clazz);
                        }
                    }

                }
            });

            setMethod.invoke(stmt, stream);
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @SuppressWarnings("resource")
    public static InputStream getLocalInfileInputStream(Statement stmt) {
        if (stmt instanceof IStatement) {
            return ((IStatement) stmt).getLocalInfileInputStream();
        }

        if (stmt instanceof DruidPooledStatement) {
            stmt = ((DruidPooledStatement) stmt).getStatement();
            return getLocalInfileInputStream(stmt);
        }

        if (stmt instanceof StatementProxy) {
            try {
                stmt = stmt.unwrap(Statement.class);
                return getLocalInfileInputStream(stmt);
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
            }
        }

        try {
            Statement stmtnew = stmt.unwrap(Statement.class);
            if (!stmtnew.getClass().equals(stmt.getClass())) {
                return getLocalInfileInputStream(stmtnew);
            } else {
                stmt = stmtnew;
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        }

        try {
            final Class<?> clazz = stmt.getClass();
            Method setMethod = getMethodCaches.get(clazz, new Callable<Method>() {

                @Override
                public Method call() throws Exception {

                    Class c = clazz;
                    while (true) {
                        try {
                            Method setMethod = c.getDeclaredMethod("getLocalInfileInputStream");
                            if (setMethod != null) {
                                setMethod.setAccessible(true);
                            }
                            return setMethod;
                        } catch (Exception ex) {

                        }

                        c = c.getSuperclass();

                        if (c == null) {
                            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                                "get getLocalInfileInputStream error, clazz:" + clazz);
                        }
                    }
                }
            });

            return (InputStream) setMethod.invoke(stmt, new Object[] {});
        } catch (Throwable e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
