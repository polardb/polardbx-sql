//package com.alibaba.polardbx.executor.utils;
//
//import GeneralUtil;
//import com.alibaba.polardbx.optimizer.chunk.Chunk;
//import ExecutionContext;
//import Function;
//import DynamicParamExpression;
//import IExpression;
//import ScalarFunction;
//import Row;
//import org.apache.calcite.rel.core.CorrelationId;
//import org.apache.calcite.sql.fun.SqlQuantifyOperator;
//
//import java.util.Arrays;
//import java.util.List;
//
//public class NoScalarSubqueryApply extends SubqueryApply {
//
//    private Object[] actualArgs;
//    private int actualArgsIndex = 0;
//    private boolean isLeftContainsNull = false;
//
//    private boolean hasNull = false;
//    private Function function;
//
//    public NoScalarSubqueryApply(String subqueryId,
//                                 Chunk.ChunkRow inputChunk,
//                                 DynamicParamExpression paramExpr,
//                                 ExecutionContext executionContext,
//                                 CorrelationId correlateId,
//                                 boolean isHandlerApply) {
//        super(subqueryId, inputChunk, paramExpr, executionContext, correlateId, isHandlerApply);
//    }
//
//    @Override
//    public void prepare() {
//        super.prepare();
//        initActualArgs();
//        switch (subqueryKind) {
//        case SOME:
//        case ALL:
//            function = new Function();
//            function.setFunctionName(((SqlQuantifyOperator) paramExpr.getSubqueryOp()).comparisonKind.sql);
//            return;
//        case IN:
//        case NOT_IN:
//            function = new Function();
//            function.setFunctionName("EQUAL");
//            return;
//        default:
//            throw GeneralUtil.nestedException("Unsupported Subquery type " + subqueryKind);
//        }
//    }
//
//    void initActualArgs() {
//        List<IExpression> args = paramExpr.getSubqueryOperands();
//        if (paramExpr.getSubqueryOperands() != null
//            && paramExpr.getSubqueryOperands().size() > 0) {
//            actualArgs = new Object[args.size() + 1];
//        } else {
//            throw GeneralUtil.nestedException("Subquery operands number error:"
//                + paramExpr.getSubqueryOperands());
//        }
//        for (IExpression iExpression : args) {
//            Object o = iExpression.eval(inputRow);
//            actualArgs[actualArgsIndex++] = o;
//            if (o == null) {
//                isLeftContainsNull = true;
//            }
//        }
//    }
//
//    @Override
//    public void process() {
//        Row row = cursor.next();
//        if (row == null) {
//            if (localMode && !cursor.isFinished()) {
//                return;
//            }
//        } else {
//            emptyResult = false;
//        }
//
//        switch (subqueryKind) {
//        case SOME:
//            processSome(row);
//            return;
//        case ALL:
//            processAll(row);
//            return;
//        case IN:
//            processIn(row);
//            return;
//        case NOT_IN:
//            processNotIn(row);
//            return;
//        default:
//            throw GeneralUtil.nestedException("Unsupported Subquery type " + subqueryKind);
//        }
//    }
//
//    void processNotIn(Row row) {
//        if (function == null) {
//            function = new Function();
//            function.setFunctionName("EQUAL");
//        }
//        if (row == null) {
//            // if subquery return empty set ,return true; Everone is in an empty set.
//            if (hasNull) {
//                paramExpr.setValue(null);
//            } else {
//                paramExpr.setValue(1);
//            }
//            isFinish = true;
//            return;
//        }
//        if (isLeftContainsNull) {
//            // if operator contains null, return null;
//            paramExpr.setValue(null);
//            isFinish = true;
//            return;
//        }
//
//        actualArgs[actualArgsIndex] = row.getObject(0);
//        if (actualArgs[actualArgsIndex] == null) {
//            hasNull = true;
//        }
//        function.setArgs(Arrays.asList(actualArgs));
//        ScalarFunction wrappedFunctionEqual = (ScalarFunction) function.getExtraFunction();
//        Object o = wrappedFunctionEqual.compute(actualArgs, subQueryEc);
//        if (o != null && (Long) o == 1) {// return false if any
//            // row match.
//            paramExpr.setValue(0);
//            isFinish = true;
//            return;
//        }
//    }
//
//    void processAll(Row row) {
//        if (row == null) {
//            // if subquery is empty set , subquery with all return true;
//            if (hasNull) {
//                // if all row match but has null,return null;
//                paramExpr.setValue(null);
//            } else {
//                // if all row match without null, return true;
//                paramExpr.setValue(1);
//            }
//            isFinish = true;
//            return;
//        }
//
//        if (isLeftContainsNull) {
//            // if operators contains null, return null;
//            paramExpr.setValue(null);
//            isFinish = true;
//            return;
//        }
//        hasNull = false;
//
//        actualArgs[actualArgsIndex] = row.getObject(0);
//        if (actualArgs[actualArgsIndex] == null) {
//            hasNull = true;
//        }
//        function.setArgs(Arrays.asList(actualArgs));
//        ScalarFunction wrappedFunction = (ScalarFunction) function.getExtraFunction();
//        Object o = wrappedFunction.compute(actualArgs, subQueryEc);
//        if (o != null && (Long) o != 1) {
//            // if one row donot match ,return false;
//            paramExpr.setValue(0);
//            isFinish = true;
//            return;
//        }
//    }
//
//    void processIn(Row row) {
//        if (row == null) {
//            // if subquery return empty set ,return false; No one in an empty set.
//            if (emptyResult) {
//                paramExpr.setValue(0);
//            } else {
//                if (hasNull) {
//                    // return null if subquery return rows with a null one inside.
//                    paramExpr.setValue(null);
//                } else {
//                    // return false if subquery contains no null data.
//                    paramExpr.setValue(1);
//                }
//            }
//            isFinish = true;
//            return;
//        }
//
//        if (isLeftContainsNull) {
//            // if operator contains null, return null;
//            paramExpr.setValue(null);
//            isFinish = true;
//            return;
//        }
//
//        actualArgs[actualArgsIndex] = row.getObject(0);
//        if (actualArgs[actualArgsIndex] == null) {
//            hasNull = true;
//        }
//        function.setArgs(Arrays.asList(actualArgs));
//        ScalarFunction wrappedFunctionEqual = (ScalarFunction) function.getExtraFunction();
//        Object o = wrappedFunctionEqual.compute(actualArgs, subQueryEc);
//        if (o != null && (Long) o == 1) {
//            // return true if any row match.
//            paramExpr.setValue(1);
//            isFinish = true;
//            return;
//        }
//    }
//
//
//
//    void processSome(Row row) {
//        if (row == null) {
//            // if subquery is empty set , subquery with some return false;
//            if (hasNull) {
//                // if we got 0 match plus subquery rows contains null, return null;
//                paramExpr.setValue(null);
//            } else {
//                // if we got 0 match and subquery had none null value,return false;
//                paramExpr.setValue(0);
//            }
//            isFinish = true;
//            return;
//        }
//        if (isLeftContainsNull) {
//            // if operators contains null, return null;
//            paramExpr.setValue(null);
//            isFinish = true;
//            return;
//        }
//        actualArgs[actualArgsIndex] = row.getObject(0);
//        if (actualArgs[actualArgsIndex] == null) {
//            hasNull = true;
//        }
//        function.setArgs(Arrays.asList(actualArgs));
//        ScalarFunction wrappedFunction = (ScalarFunction) function.getExtraFunction();
//        Object o = wrappedFunction.compute(actualArgs, subQueryEc);
//        if (o != null && (Long) o == 1) {
//            // return true if any row match;
//            paramExpr.setValue(true);
//            // return true if any row match;
//            paramExpr.setValue(1);
//            isFinish = true;
//            return;
//        }
//    }
//}
