package com.alibaba.polardbx.optimizer.core.function.calc.scalar.string;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;

import java.util.ArrayList;
import java.util.List;

/**
 * @author chenhui.lch
 *
 * PartRoute
 * <p>
 * usage:
 * PartRoute('db_name','tbl_name','a', '2020-12-12','1234')
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

        List<String> dbAndTb = new ArrayList<>();
        for (int i = 0; i < 2; i++) {
            String argStr = DataTypeUtil.convert(operandTypes.get(i), DataTypes.StringType, args[i]);
            dbAndTb.add(argStr);
        }

        String dbName = dbAndTb.get(0);
        String tbName = dbAndTb.get(1);
        List<Object> pointValue = new ArrayList<>();
        List<DataType> pointOpTypes = new ArrayList<>();
        for (int i = 2; i < args.length; i++) {
            pointValue.add(args[i]);
            pointOpTypes.add(operandTypes.get(i));
        }

        StringBuilder partRsStrSb = new StringBuilder("");
        ExecutionContext[] newEc = new ExecutionContext[1];
        PartitionPruneStep step =
            PartitionPruneStepBuilder.generatePointSelectPruneStepInfo(dbName, tbName, pointValue, pointOpTypes, ec,
                newEc);
        PartPrunedResult result = PartitionPruner.doPruningByStepInfo(step, newEc[0]);
        List<PhysicalPartitionInfo> partitions = result.getPrunedParttions();
        for (int i = 0; i < partitions.size(); i++) {
            if (i > 0) {
                partRsStrSb.append(",");
            }
            partRsStrSb.append(partitions.get(i).getPartName());
        }
        return partRsStrSb.toString();
    }
}
