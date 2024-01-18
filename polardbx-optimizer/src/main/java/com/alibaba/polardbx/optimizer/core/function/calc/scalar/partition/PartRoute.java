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

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import com.alibaba.polardbx.optimizer.partition.util.PartTupleRouter;

import java.util.ArrayList;
import java.util.List;

/**
 *
 */

/**
 * @author chenghui.lch
 * <p>
 * PartRoute
 * <p>
 *
 * <pre>
 *
 * usage:
 * for partiton(partCol: c1,c2):
 *  part_route('db_name','tbl_name', c1_val, c2_val)
 *
 * for partition(partCol:c1,c2) with subpartition(subPartCol:c2,c3)
 *  part_route('db_name','tbl_name',c1_val, c2_val, c2_val, c3_val)
 *
 * </pre>
 */
public class PartRoute extends AbstractScalarFunction {
    public PartRoute(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"PART_ROUTE"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

//        List<String> dbAndTb = new ArrayList<>();
//        for (int i = 0; i < 2; i++) {
//            String argStr = DataTypeUtil.convert(getOperandType(i), DataTypes.StringType, args[i]);
//            dbAndTb.add(argStr);
//        }
//
//        String dbName = dbAndTb.get(0);
//        String tbName = dbAndTb.get(1);
//
//        String partColListStr = (String) args[2];
//        String[] partColArr = partColListStr.split(",");
//
//        PartitionInfo partInfo = ec.getSchemaManager(dbName).getTable(tbName).getPartitionInfo();
//        PartitionByDefinition partByDef = partInfo.getPartitionBy();
//        boolean useSubPartBy = partByDef.getSubPartitionBy() != null;
//
//
//
//        List<Object> pointValue = new ArrayList<>();
//        List<DataType> pointOpTypes = new ArrayList<>();
//        for (int i = 2; i < args.length; i++) {
//            pointValue.add(args[i]);
//            pointOpTypes.add(getOperandType(i));
//        }
//
//        StringBuilder partRsStrSb = new StringBuilder("");
//        ExecutionContext[] newEc = new ExecutionContext[1];
//        PartitionPruneStep step =
//            PartitionPruneStepBuilder.generatePointSelectPruneStepInfo(dbName, tbName, pointValue, pointOpTypes, ec,
//                newEc);
//        PartPrunedResult result = PartitionPruner.doPruningByStepInfo(step, newEc[0]);
//        List<PhysicalPartitionInfo> partitions = result.getPrunedParttions();
//        for (int i = 0; i < partitions.size(); i++) {
//            if (i > 0) {
//                partRsStrSb.append(",");
//            }
//            partRsStrSb.append(partitions.get(i).getPartName());
//        }
//        return partRsStrSb.toString();

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

        PartitionInfo partInfo = ec.getSchemaManager(dbName).getTable(tbName).getPartitionInfo();
        PartTupleRouter router = new PartTupleRouter(partInfo, ec);
        router.init();
        int partColCnt = partInfo.getPartitionBy().getPartitionFieldList().size();
        int subPartColCnt = 0;
        boolean useSubPart = partInfo.getPartitionBy().getSubPartitionBy() != null;
        if (useSubPart) {
            subPartColCnt = partInfo.getPartitionBy().getSubPartitionBy().getPartitionFieldList().size();
        }
        if (pointValue.size() != (partColCnt + subPartColCnt)) {
            throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_INVALID_PARAMS,
                "Tuple values should contains both partition column value and subpartition column value");
        }
        List<List<Object>> tupleValList = new ArrayList<>();
        List<Object> partTupleVal = new ArrayList<>();
        for (int i = 0; i < partColCnt; i++) {
            partTupleVal.add(pointValue.get(i));
        }
        tupleValList.add(partTupleVal);
        if (useSubPart) {
            List<Object> subPartTupleVal = new ArrayList<>();
            for (int i = 0; i < subPartColCnt; i++) {
                subPartTupleVal.add(pointValue.get(i + partColCnt));
            }
            tupleValList.add(subPartTupleVal);
        }
        PhysicalPartitionInfo phyPartInfo = router.routeTuple(tupleValList);
        if (phyPartInfo == null) {
            return "";
        }
        return phyPartInfo.getPartName();
    }
}
