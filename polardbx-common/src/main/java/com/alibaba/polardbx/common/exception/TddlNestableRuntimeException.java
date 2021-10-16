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

package com.alibaba.polardbx.common.exception;

import com.alibaba.polardbx.common.exception.nest.Nestable;
import com.alibaba.polardbx.common.exception.nest.NestableDelegate;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.concurrent.ExecutionException;


public class TddlNestableRuntimeException extends RuntimeException
    implements Nestable {

    private static final long serialVersionUID = -363994535286446190L;

    private String SQLState;

    protected int vendorCode;

    protected NestableDelegate delegate =
        new NestableDelegate(this);

    protected Object extraParams;

    public TddlNestableRuntimeException() {
        super();
    }

    public TddlNestableRuntimeException(String msg) {
        super(msg);
    }

    public TddlNestableRuntimeException(Throwable cause) {
        super(null, cause);
        buildSQLMessage(cause);
    }

    public TddlNestableRuntimeException(String msg, Throwable cause) {
        super(msg, cause);
        buildSQLMessage(cause);
    }

    public TddlNestableRuntimeException(Throwable cause, Object extraParams) {
        super(null, cause);
        buildSQLMessage(cause);
        this.extraParams = extraParams;
    }

    @Override
    public String getMessage() {
        if (super.getMessage() != null) {
            return super.getMessage();
        } else if (getCause() != null) {
            Throwable ca = getCause();

            if (ca != null) {
                return ca.getMessage();
            } else {
                return null;
            }

        } else {
            return null;
        }
    }

    @Override
    public String getMessage(int index) {
        if (index == 0) {
            return super.getMessage();
        }
        return delegate.getMessage(index);
    }

    @Override
    public String[] getMessages() {
        return delegate.getMessages();
    }

    @Override
    public Throwable getThrowable(int index) {
        return delegate.getThrowable(index);
    }

    @Override
    public int getThrowableCount() {
        return delegate.getThrowableCount();
    }

    @Override
    public Throwable[] getThrowables() {
        return delegate.getThrowables();
    }

    @Override
    public int indexOfThrowable(Class type) {
        return delegate.indexOfThrowable(type, 0);
    }

    @Override
    public int indexOfThrowable(Class type, int fromIndex) {
        return delegate.indexOfThrowable(type, fromIndex);
    }

    @Override
    public void printStackTrace() {
        delegate.printStackTrace();
    }

    @Override
    public void printStackTrace(PrintStream out) {
        delegate.printStackTrace(out);
    }

    @Override
    public void printStackTrace(PrintWriter out) {
        delegate.printStackTrace(out);
    }

    @Override
    public final void printPartialStackTrace(PrintWriter out) {
        super.printStackTrace(out);
    }

    @Override
    public String toString() {
        return super.getLocalizedMessage();
    }

    private void buildSQLMessage(Throwable e) {
        if (e instanceof SQLException) {
            this.vendorCode = ((SQLException) e).getErrorCode();
            this.SQLState = ((SQLException) e).getSQLState();

            if (this.SQLState != null && SQLState.equalsIgnoreCase("08S01")) {
                this.SQLState = null;
            }
        } else if (e instanceof TddlNestableRuntimeException) {
            this.vendorCode = ((TddlNestableRuntimeException) e).getErrorCode();
            this.SQLState = ((TddlNestableRuntimeException) e).getSQLState();
        } else if (e instanceof ExecutionException) {
            buildSQLMessage(e.getCause());
        }
    }

    public String getSQLState() {
        return SQLState;
    }

    public int getErrorCode() {
        return vendorCode;
    }

    public void setExtraParams(Object extraParams) {
        this.extraParams = extraParams;
    }

}
