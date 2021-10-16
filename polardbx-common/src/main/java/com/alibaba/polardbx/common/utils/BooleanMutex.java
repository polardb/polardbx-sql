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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.locks.AbstractQueuedSynchronizer;


public class BooleanMutex {

    private Sync sync;

    public BooleanMutex(){
        sync = new Sync();
        set(false);
    }

    public BooleanMutex(Boolean mutex){
        sync = new Sync();
        set(mutex);
    }


    public void get() throws InterruptedException {
        sync.innerGet();
    }


    public void get(long timeout, TimeUnit unit) throws InterruptedException, TimeoutException {
        sync.innerGet(unit.toNanos(timeout));
    }

    public void set(Boolean mutex) {
        if (mutex) {
            sync.innerSetTrue(true);
        } else {
            sync.innerSetFalse(true);
        }
    }

    public void trySet(Boolean mutex) {
        if (mutex) {
            sync.innerSetTrue(false);
        } else {
            sync.innerSetFalse(false);
        }
    }

    public boolean state() {
        return sync.innerState();
    }

    private final class Sync extends AbstractQueuedSynchronizer {

        private static final long serialVersionUID = 2559471934544126329L;

        private static final int  TRUE             = 1;

        private static final int  FALSE            = 2;

        private boolean isTrue(int state) {
            return (state & TRUE) != 0;
        }

        protected int tryAcquireShared(int state) {

            return isTrue(getState()) ? 1 : -1;
        }

        protected boolean tryReleaseShared(int ignore) {

            return true;
        }

        boolean innerState() {
            return isTrue(getState());
        }

        void innerGet() throws InterruptedException {
            acquireSharedInterruptibly(0);
        }

        void innerGet(long nanosTimeout) throws InterruptedException, TimeoutException {
            if (!tryAcquireSharedNanos(0, nanosTimeout)) throw new TimeoutException();
        }

        void innerSetTrue(boolean lazySet) {
            while (lazySet) {
                int s = getState();
                if (s == TRUE) {
                    return;
                }
                if (compareAndSetState(s, TRUE)) {
                    releaseShared(0);
                    return;
                }
            }
        }

        void innerSetFalse(boolean lazySet) {
            while (lazySet) {
                int s = getState();
                if (s == FALSE) {
                    return;
                }
                if (compareAndSetState(s, FALSE)) {
                    return;
                }
            }
        }

    }
}
