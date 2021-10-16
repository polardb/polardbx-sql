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

package com.alibaba.polardbx.optimizer.core.row;

import com.alibaba.polardbx.optimizer.core.CursorMeta;

import java.util.ArrayList;
import java.util.List;

/**
 * @author mengshi.sunmengshi 2013-12-3 上午11:05:48
 * @since 5.0.0
 */
public class JoinRow extends AbstractRow implements Row {

    private int rightCursorOffset;
    private Row leftRow;
    private Row rightRow;

    public JoinRow(int rightCursorOffset, Row leftRow, Row rightRow, CursorMeta cursorMeta, int rightLenth) {
        super(cursorMeta);
        this.rightCursorOffset = rightCursorOffset;
        this.leftRow = leftRow;
        this.rightRow = rightRow;
        this.colNum = rightCursorOffset + rightLenth;
    }

    public JoinRow(int rightCursorOffset, Row leftRow, Row rightRow, CursorMeta cursorMeta) {
        super(cursorMeta);
        this.rightCursorOffset = rightCursorOffset;
        this.leftRow = leftRow;
        this.rightRow = rightRow;
        this.colNum = rightCursorOffset + rightRow.getColNum();
    }

    public int getRightCursorOffset() {
        return rightCursorOffset;
    }

    public void setRightCursorOffset(int rightCursorOffset) {
        this.rightCursorOffset = rightCursorOffset;
    }

    public Row getLeftRowSet() {
        return leftRow;
    }

    public void setLeftRowSet(Row leftRow) {
        this.leftRow = leftRow;
    }

    public Row getRightRowSet() {
        return rightRow;
    }

    public void setRightRowSet(Row rightRow) {
        this.rightRow = rightRow;
    }

    @Override
    public Object getObject(int index) {
        if (rightCursorOffset <= index) {
            if (rightRow == null) {
                return null;
            }
            return rightRow.getObject(index - rightCursorOffset);
        }

        if (leftRow == null) {
            return null;
        }

        return leftRow.getObject(index);
    }

    @Override
    public void setObject(int index, Object value) {
        if (rightCursorOffset <= index) {
            rightRow.setObject(index - rightCursorOffset, value);
            return;
        }
        leftRow.setObject(index, value);
    }

    @Override
    public List<Object> getValues() {
        ArrayList<Object> values = new ArrayList<Object>();
        if (leftRow == null && rightRow == null) {
            int size = this.getParentCursorMeta().getColumns().size();
            for (int i = 0; i < size; i++) {
                values.add(null);
            }
        } else if (rightRow == null) {
            values.addAll(leftRow.getValues());
            for (int i = 0; i < colNum - leftRow.getValues().size(); i++) {
                values.add(null);
            }

        } else if (leftRow == null) {

            for (int i = 0; i < this.getParentCursorMeta().getColumns().size() - rightRow.getValues().size(); i++) {
                values.add(null);
            }
            values.addAll(rightRow.getValues());

        } else {
            values.addAll(leftRow.getValues());
            values.addAll(rightRow.getValues());
        }
        return values;
    }

    @Override
    public long estimateSize() {
        return leftRow.estimateSize() + rightRow.estimateSize();
    }
}
