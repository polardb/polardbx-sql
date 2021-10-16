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

package com.alibaba.polardbx.executor.mdl;

import javax.validation.constraints.NotNull;

/**
 * 获取到的锁对象
 *
 * @author chenmo.cm
 */
public final class MdlTicket {

    final MdlType type;
    final MdlDuration duration;
    final MdlLock lock;
    /**
     * reference of context to which this ticket belongs
     */
    final MdlContext context;
    final long stamp;
    /**
     * mark whether this lock already unlocked
     */
    private volatile boolean validate = true;

    public MdlTicket(@NotNull MdlRequest request, @NotNull MdlLock lock, @NotNull MdlContext context, long stamp) {
        this.type = request.type;
        this.duration = request.duration;
        this.lock = lock;
        this.context = context;
        this.stamp = stamp;
    }

    public MdlTicket(@NotNull MdlType type, @NotNull MdlDuration duration, @NotNull MdlLock lock,
                     @NotNull MdlContext context, long stamp) {
        this.type = type;
        this.duration = duration;
        this.lock = lock;
        this.context = context;
        this.stamp = stamp;
    }

    public synchronized boolean isValidate() {
        return validate;
    }

    public synchronized void setValidate(boolean validate) {
        this.validate = validate;
    }

    public MdlType getType() {
        return type;
    }

    public MdlDuration getDuration() {
        return duration;
    }

    public MdlLock getLock() {
        return lock;
    }

    public MdlContext getContext() {
        return context;
    }

    public long getStamp() {
        return stamp;
    }

    @Override
    public String toString() {
        return "MdlTicket{" + "type=" + type + ", duration=" + duration + ", lock=" + lock + ", context=" + context
            + ", stamp=" + stamp + ", validate=" + validate + '}';
    }
}
