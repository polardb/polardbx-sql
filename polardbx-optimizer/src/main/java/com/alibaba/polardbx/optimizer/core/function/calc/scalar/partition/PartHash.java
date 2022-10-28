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

package com.alibaba.polardbx.optimizer.core.function.calc.scalar.partition;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenghui.lch
 *
 * PartRoute
 * <p>
 * usage:
 * part_hash('db_name','tbl_name','a', '2020-12-12','1234')
 */
public class PartHash extends AbstractScalarFunction {

    public PartHash(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PART_HASH"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        List<String> dbAndTb = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            String argStr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            dbAndTb.add(argStr);
        }

        String dbName = dbAndTb.get(0);
        String tbName = dbAndTb.get(1);
        List<Object> pointValue = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            pointValue.add(args[i]);
        }

        StringBuilder partRsStrSb = new StringBuilder("");
        PartitionInfo partInfo = ec.getSchemaManager(dbName).getTable(tbName).getPartitionInfo();
        PartTupleRouter router = new PartTupleRouter(partInfo, ec);
        router.init();
        SearchDatumInfo searchDatum = router.calcSearchDatum(pointValue);
        for (int i = 0; i < searchDatum.getDatumInfo().length; i++) {
            if (i > 0) {
                partRsStrSb.append(",");
            }
            partRsStrSb.append(searchDatum.getDatumInfo()[i].getValue().stringValue().toStringUtf8());
        }
        return String.join(",", partRsStrSb);
    }
}
