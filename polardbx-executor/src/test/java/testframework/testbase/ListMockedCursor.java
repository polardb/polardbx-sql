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

package testframework.testbase;

import com.alibaba.polardbx.executor.cursor.AbstractCursor;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.List;

/**
 * Created by chuanqin on 17/9/26.
 */
public class ListMockedCursor extends AbstractCursor {

    private List<Object[]> rawRow;
    private int cursorIndex;

    public ListMockedCursor(List<Object[]> rawRow, List<ColumnMeta> columns) {
        super(false);
        this.rawRow = rawRow;
        this.returnColumns = columns;
    }

    @Override
    public Row doNext() {
        if (cursorIndex < rawRow.size()) {
            return new ArrayRow(null, rawRow.get(cursorIndex++));
        } else {
            return null;
        }
    }

    @Override
    public List<Throwable> doClose(List<Throwable> exceptions) {
        return null;
    }
}
