
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

package com.alibaba.polardbx.common.exception.nest;

import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.PrintStream;
import java.io.PrintWriter;


public interface Nestable {

    public Throwable getCause();

    public String getMessage();

    public String getMessage(int index);

    public String[] getMessages();

    public Throwable getThrowable(int index);

    public int getThrowableCount();

    public Throwable[] getThrowables();

    public int indexOfThrowable(Class type);

    public int indexOfThrowable(Class type, int fromIndex);

    public void printStackTrace(PrintWriter out);

    public void printStackTrace(PrintStream out);

    public void printPartialStackTrace(PrintWriter out);

}
