package com.alibaba.polardbx.executor.function.calc;

import com.alibaba.polardbx.common.datatype.RowValue;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ObjectBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.SqlCartesianFunction;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.ArrayList;
import java.util.List;

public class Cartesian extends AbstractScalarFunction {

    public Cartesian(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {

        ObjectBlockBuilder[] blockBuilders = new ObjectBlockBuilder[args.length];
        int chunkLimit = ec.getParamManager().getInt(ConnectionParams.CHUNK_SIZE);
        for (int i = 0; i < args.length; i++) {
            blockBuilders[i] = new ObjectBlockBuilder(chunkLimit);
        }
        List<List<Object>> inputLists = new ArrayList<>();
        for (Object arg : args) {
            if (arg instanceof List) {
                inputLists.add((List<Object>) arg);
            } else if (arg instanceof RowValue) {
                // change arg from RowValue to list
                inputLists.add(((RowValue) arg).getValues());
            } else {
                throw new IllegalArgumentException("Each element in the args array must be a List.");
            }
        }
        // 开始递归计算笛卡尔积
        computeCartesianProduct(inputLists, blockBuilders, new ArrayList<>(), 0);

        Block[] blocks = new Block[args.length];
        for (int i = 0; i < blockBuilders.length; i++) {
            blocks[i] = blockBuilders[i].build();
        }

        return new Chunk(blocks);
    }

    private static void computeCartesianProduct(List<List<Object>> lists, ObjectBlockBuilder[] result,
                                                List<Object> current, int depth) {
        if (depth == lists.size()) {
            for (int i = 0; i < current.size(); i++) {
                result[i].writeObject(current.get(i));
            }
            return;
        }

        for (Object element : lists.get(depth)) {
            current.add(element);
            computeCartesianProduct(lists, result, current, depth + 1);
            current.remove(current.size() - 1);
        }
    }

    public Object evaluateResult(Object[] args, ExecutionContext ec) {
        return compute(args, ec);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {SqlCartesianFunction.name};
    }
}