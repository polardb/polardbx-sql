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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.common.RecycleBinManager.RecycleBinParam;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.List;

/**
 * @author desai
 */
public class LogicalShowRecyclebinHandler extends HandlerCommon {

    public LogicalShowRecyclebinHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        String appName = executionContext.getAppName();
        RecycleBin bin = RecycleBinManager.instance.getByAppName(appName);
        if (bin == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_RECYCLEBIN_EXECUTE, "can't find recycle bin");
        }
        ArrayResultCursor result = new ArrayResultCursor("RECYCLEBIN");
        result.addColumn("Id", DataTypes.IntegerType);
        result.addColumn("NAME", DataTypes.StringType);
        result.addColumn("ORIGINAL_NAME", DataTypes.StringType);
        result.addColumn("CREATED", DataTypes.DatetimeType);
        result.initMeta();

        int index = 0;
        for (RecycleBinParam param : bin.getAll(false)) {
            result.addRow(new Object[] {index++, param.name, param.originalName, param.created});
        }

        return result;
    }
}
