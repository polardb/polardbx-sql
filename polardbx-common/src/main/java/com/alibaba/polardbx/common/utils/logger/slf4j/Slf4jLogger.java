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

package com.alibaba.polardbx.common.utils.logger.slf4j;

import com.alibaba.polardbx.common.utils.logger.Logger;

import java.io.Serializable;

public class Slf4jLogger implements Logger, Serializable {

    private static final long serialVersionUID = 1L;

    private final org.slf4j.Logger logger;

    public Slf4jLogger(org.slf4j.Logger logger) {
        this.logger = logger;
    }

    public void trace(String msg) {
        logger.trace(msg);
    }

    public void trace(Throwable e) {
        logger.trace(e.getMessage(), e);
    }

    public void trace(String msg, Throwable e) {
        logger.trace(msg, e);
    }

    public void debug(String msg) {
        logger.debug(msg);
    }

    public void debug(Throwable e) {
        logger.debug(e.getMessage(), e);
    }

    public void debug(String msg, Throwable e) {
        logger.debug(msg, e);
    }

    public void info(String msg) {
        logger.info(msg);
    }

    public void info(Throwable e) {
        logger.info(e.getMessage(), e);
    }

    public void info(String msg, Throwable e) {
        logger.info(msg, e);
    }

    public void warn(String msg) {
        logger.warn(msg);
    }

    public void warn(Throwable e) {
        logger.warn(e.getMessage(), e);
    }

    public void warn(String msg, Throwable e) {
        logger.warn(msg, e);
    }

    public void error(String msg) {
        logger.error(msg);
    }

    public void error(Throwable e) {
        logger.error(e.getMessage(), e);
    }

    public void error(String msg, Throwable e) {
        logger.error(msg, e);
    }

    public boolean isTraceEnabled() {
        return logger.isTraceEnabled();
    }

    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    public boolean isInfoEnabled() {
        return logger.isInfoEnabled();
    }

    public boolean isWarnEnabled() {
        return logger.isWarnEnabled();
    }

    public boolean isErrorEnabled() {
        return logger.isErrorEnabled();
    }

    public Object getDelegate() {
        return logger;
    }

}
