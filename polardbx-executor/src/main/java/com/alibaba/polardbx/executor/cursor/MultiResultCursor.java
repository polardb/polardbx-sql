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

package com.alibaba.polardbx.executor.cursor;

import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

/**
 * @author agapple 2014年10月31日 下午11:37:25
 * @since 5.1.13
 */
public class MultiResultCursor extends ResultCursor {

    private List<ResultCursor> resultCursors; // tddl逻辑执行多语句的结果

    public MultiResultCursor(List<ResultCursor> cursors) {
        super();
        this.resultCursors = cursors;
    }

    @Override
    public Row doNext() {
        throw new UnsupportedOperationException();
    }

    public List<ResultCursor> getResultCursors() {
        return resultCursors;
    }

}
