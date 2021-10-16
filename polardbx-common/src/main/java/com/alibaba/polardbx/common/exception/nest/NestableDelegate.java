
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

import org.apache.commons.lang.SystemUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import java.io.PrintStream;
import java.io.PrintWriter;
import java.io.Serializable;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.StringTokenizer;

public class NestableDelegate implements Serializable {

    private static final long serialVersionUID = 1L;

    private transient static final String MUST_BE_THROWABLE =
        "The Nestable implementation passed to the NestableDelegate(Nestable) "
            + "constructor must extend java.lang.Throwable";

    private Throwable nestable = null;

    public static boolean topDown = true;

    public static boolean trimStackFrames = true;

    public static boolean matchSubclasses = true;

    public NestableDelegate(Nestable nestable) {
        if (nestable instanceof Throwable) {
            this.nestable = (Throwable) nestable;
        } else {
            throw new IllegalArgumentException(MUST_BE_THROWABLE);
        }
    }

    public String getMessage(int index) {
        Throwable t = this.getThrowable(index);
        if (Nestable.class.isInstance(t)) {
            return ((Nestable) t).getMessage(0);
        }
        return t.getMessage();
    }

    public String getMessage(String baseMsg) {
        Throwable nestedCause = ExceptionUtils.getCause(this.nestable);
        String causeMsg = nestedCause == null ? null : nestedCause.getMessage();
        if (nestedCause == null || causeMsg == null) {
            return baseMsg;
        }
        if (baseMsg == null) {
            return causeMsg;
        }
        return baseMsg + ": " + causeMsg;
    }

    public String[] getMessages() {
        Throwable[] throwables = this.getThrowables();
        String[] msgs = new String[throwables.length];
        for (int i = 0; i < throwables.length; i++) {
            msgs[i] = (Nestable.class.isInstance(throwables[i]) ? ((Nestable) throwables[i]).getMessage(0) :
                throwables[i].getMessage());
        }
        return msgs;
    }

    public Throwable getThrowable(int index) {
        if (index == 0) {
            return this.nestable;
        }
        Throwable[] throwables = this.getThrowables();
        return throwables[index];
    }

    public int getThrowableCount() {
        return ExceptionUtils.getThrowableCount(this.nestable);
    }

    public Throwable[] getThrowables() {
        return ExceptionUtils.getThrowables(this.nestable);
    }

    public int indexOfThrowable(Class type, int fromIndex) {
        if (type == null) {
            return -1;
        }
        if (fromIndex < 0) {
            throw new IndexOutOfBoundsException("The start index was out of bounds: " + fromIndex);
        }
        Throwable[] throwables = ExceptionUtils.getThrowables(this.nestable);
        if (fromIndex >= throwables.length) {
            throw new IndexOutOfBoundsException("The start index was out of bounds: " + fromIndex + " >= "
                + throwables.length);
        }
        if (matchSubclasses) {
            for (int i = fromIndex; i < throwables.length; i++) {
                if (type.isAssignableFrom(throwables[i].getClass())) {
                    return i;
                }
            }
        } else {
            for (int i = fromIndex; i < throwables.length; i++) {
                if (type.equals(throwables[i].getClass())) {
                    return i;
                }
            }
        }
        return -1;
    }

    public void printStackTrace() {
        printStackTrace(System.err);
    }

    public void printStackTrace(PrintStream out) {
        synchronized (out) {
            PrintWriter pw = new PrintWriter(out, false);
            printStackTrace(pw);

            pw.flush();
        }
    }

    public void printStackTrace(PrintWriter out) {
        Throwable throwable = this.nestable;

        if (ExceptionUtils.isThrowableNested()) {
            if (throwable instanceof Nestable) {
                ((Nestable) throwable).printPartialStackTrace(out);
            } else {
                throwable.printStackTrace(out);
            }
            return;
        }

        List stacks = new ArrayList();
        while (throwable != null) {
            String[] st = getStackFrames(throwable);
            stacks.add(st);
            throwable = ExceptionUtils.getCause(throwable);
        }

        String separatorLine = "Caused by: ";
        if (!topDown) {
            separatorLine = "Rethrown as: ";
            Collections.reverse(stacks);
        }

        if (trimStackFrames) {
            trimStackFrames(stacks);
        }

        synchronized (out) {
            for (Iterator iter = stacks.iterator(); iter.hasNext(); ) {
                String[] st = (String[]) iter.next();
                for (int i = 0, len = st.length; i < len; i++) {
                    out.println(st[i]);
                }
                if (iter.hasNext()) {
                    out.print(separatorLine);
                }
            }
        }
    }

    protected String[] getStackFrames(Throwable t) {
        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw, true);

        if (t instanceof Nestable) {
            ((Nestable) t).printPartialStackTrace(pw);
        } else {
            t.printStackTrace(pw);
        }
        return getStackFrames(sw.getBuffer().toString());
    }

    protected void trimStackFrames(List stacks) {
        for (int size = stacks.size(), i = size - 1; i > 0; i--) {
            String[] curr = (String[]) stacks.get(i);
            String[] next = (String[]) stacks.get(i - 1);

            List currList = new ArrayList(Arrays.asList(curr));
            List nextList = new ArrayList(Arrays.asList(next));
            ExceptionUtils.removeCommonFrames(currList, nextList);

            int trimmed = curr.length - currList.size();
            if (trimmed > 0) {
                currList.add("\t... " + trimmed + " more");
                stacks.set(i, currList.toArray(new String[currList.size()]));
            }
        }
    }

    protected String[] getStackFrames(String stackTrace) {
        String linebreak = SystemUtils.LINE_SEPARATOR;
        StringTokenizer frames = new StringTokenizer(stackTrace, linebreak);
        List list = new ArrayList();
        while (frames.hasMoreTokens()) {
            list.add(frames.nextToken());
        }
        return (String[]) list.toArray(new String[list.size()]);
    }
}
