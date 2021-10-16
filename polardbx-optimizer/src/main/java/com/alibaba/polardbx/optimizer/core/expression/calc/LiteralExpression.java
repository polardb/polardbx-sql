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

package com.alibaba.polardbx.optimizer.core.expression.calc;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

/**
 * Representation of literal expression / value
 */
public class LiteralExpression implements IExpression, Supplier<Object> {
    public LiteralExpression(Object value) {
        this.value = value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    private Object value;

    @Override
    public Object eval(Row row) {
        return value;
    }

    @Override
    public Object get() {
        return value;
    }

    public Object eval(Row row, ExecutionContext ec) {
        return value;
    }

    @Override
    public Object evalEndPoint(Row row, ExecutionContext ec, Boolean cmpDirection, AtomicBoolean inclEndp) {
        return eval(row, ec);
    }
}