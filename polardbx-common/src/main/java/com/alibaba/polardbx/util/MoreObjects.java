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

package com.alibaba.polardbx.util;

import com.google.common.annotations.GwtCompatible;
import com.google.common.base.Preconditions;

import javax.annotation.Nullable;

@GwtCompatible
public final class MoreObjects {
    public static <T> T firstNonNull(T first, T second) {
        if (first != null) {
            return first;
        } else if (second != null) {
            return second;
        } else {
            throw new NullPointerException("Both parameters are null");
        }
    }

    public static MoreObjects.ToStringHelper toStringHelper(Object self) {
        return new MoreObjects.ToStringHelper(self.getClass().getSimpleName());
    }

    public static MoreObjects.ToStringHelper toStringHelper(String className) {
        return new MoreObjects.ToStringHelper(className);
    }

    private MoreObjects() {
    }

    public static final class ToStringHelper {
        private final String className;
        private MoreObjects.ToStringHelper.ValueHolder holderHead;
        private MoreObjects.ToStringHelper.ValueHolder holderTail;
        private boolean omitNullValues;

        private ToStringHelper(String className) {
            this.holderHead = new MoreObjects.ToStringHelper.ValueHolder();
            this.holderTail = this.holderHead;
            this.omitNullValues = false;
            this.className = (String) Preconditions.checkNotNull(className);
        }

        public MoreObjects.ToStringHelper omitNullValues() {
            this.omitNullValues = true;
            return this;
        }

        public MoreObjects.ToStringHelper add(String name, @Nullable Object value) {
            return this.addHolder(name, value);
        }

        public MoreObjects.ToStringHelper add(String name, boolean value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper add(String name, char value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper add(String name, double value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper add(String name, float value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper add(String name, int value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper add(String name, long value) {
            return this.addHolder(name, String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(@Nullable Object value) {
            return this.addHolder(value);
        }

        public MoreObjects.ToStringHelper addValue(boolean value) {
            return this.addHolder(String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(char value) {
            return this.addHolder(String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(double value) {
            return this.addHolder(String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(float value) {
            return this.addHolder(String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(int value) {
            return this.addHolder(String.valueOf(value));
        }

        public MoreObjects.ToStringHelper addValue(long value) {
            return this.addHolder(String.valueOf(value));
        }

        @Override
        public String toString() {
            boolean omitNullValuesSnapshot = this.omitNullValues;
            String nextSeparator = "";
            StringBuilder builder = (new StringBuilder(32)).append(this.className).append('{');

            for (MoreObjects.ToStringHelper.ValueHolder valueHolder = this.holderHead.next; valueHolder != null;
                 valueHolder = valueHolder.next) {
                if (!omitNullValuesSnapshot || valueHolder.value != null) {
                    builder.append(nextSeparator);
                    nextSeparator = ", ";
                    if (valueHolder.name != null) {
                        builder.append(valueHolder.name).append('=');
                    }

                    builder.append(valueHolder.value);
                }
            }

            return builder.append('}').toString();
        }

        private MoreObjects.ToStringHelper.ValueHolder addHolder() {
            MoreObjects.ToStringHelper.ValueHolder valueHolder = new MoreObjects.ToStringHelper.ValueHolder();
            this.holderTail = this.holderTail.next = valueHolder;
            return valueHolder;
        }

        private MoreObjects.ToStringHelper addHolder(@Nullable Object value) {
            MoreObjects.ToStringHelper.ValueHolder valueHolder = this.addHolder();
            valueHolder.value = value;
            return this;
        }

        private MoreObjects.ToStringHelper addHolder(String name, @Nullable Object value) {
            MoreObjects.ToStringHelper.ValueHolder valueHolder = this.addHolder();
            valueHolder.value = value;
            valueHolder.name = (String) Preconditions.checkNotNull(name);
            return this;
        }

        private static final class ValueHolder {
            String name;
            Object value;
            MoreObjects.ToStringHelper.ValueHolder next;

            private ValueHolder() {
            }
        }
    }
}
