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

package com.taobao.tddl.common.utils;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.lang.builder.ToStringStyle;


public class TddlToStringStyle extends ToStringStyle {

    private static final long serialVersionUID = -6568177374288222145L;

    private static final String DEFAULT_TIME = "yyyy-MM-dd HH:mm:ss";
    private static final String DEFAULT_DAY = "yyyy-MM-dd";

    public static final ToStringStyle TIME_STYLE = new TddlDateStyle(DEFAULT_TIME);

    public static final ToStringStyle DAY_STYLE = new TddlDateStyle(DEFAULT_DAY);

    public static final ToStringStyle DEFAULT_STYLE = TddlToStringStyle.TIME_STYLE;

    private static class TddlDateStyle extends ToStringStyle {

        private static final long serialVersionUID = 5208917932254652886L;

        private String pattern;

        public TddlDateStyle(String pattern) {
            super();
            this.setUseShortClassName(true);
            this.setUseIdentityHashCode(false);

            this.pattern = pattern;
        }

        protected void appendDetail(StringBuffer buffer, String fieldName, Object value) {

            if (value instanceof Date) {
                value = new SimpleDateFormat(pattern).format(value);
            }

            buffer.append(value);
        }
    }
}
