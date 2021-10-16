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

package com.alibaba.polardbx.rule.impl;

import java.util.Date;

/**
 * Created by chuanqin on 18/5/14.
 */
public class CrossYearDateCompensation extends Date {
    private long year;

    public CrossYearDateCompensation(long year) {
        this.year = year;
    }

    public long getCompensatedYear() {
        return year;
    }

    @Override
    public int hashCode() {
        return (int) year;
    }
}
