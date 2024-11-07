package com.alibaba.polardbx.optimizer.core.function.calc.scalar.json;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.json.JsonUtil;
import com.alibaba.polardbx.optimizer.json.exception.JsonParserException;
import com.alibaba.polardbx.optimizer.utils.FunctionUtils;
import org.apache.calcite.util.NumberUtil;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @author liugaoji
 */
public class JsonOverlaps extends JsonExtraFunction {
    public JsonOverlaps(List<DataType> operandTypes,
                        DataType resultType) {
        super(operandTypes, resultType);
    }

    public JsonOverlaps() {
        super(null, null);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"JSON_OVERLAPS"};
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 2) {
            throw JsonParserException.wrongParamCount(getFunctionNames()[0]);
        }
        //In MYSQL select json_overlaps('asdasd',NULL) return INVALID but select json_overlaps('asdasd',NULL) return NULL,
        // it's strange but we need to align with it
        String stringDoc1 = DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]);
        Object jsonDoc1;
        try {
            jsonDoc1 = JsonUtil.parse(stringDoc1);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(1, getFunctionNames()[0]);
        }
        for (Object arg : args) {
            if (FunctionUtils.isNull(arg)) {
                return null;
            }
        }
        String stringDoc2 = DataTypeUtil.convert(getOperandType(1), DataTypes.StringType, args[1]);
        Object jsonDoc2;
        try {
            jsonDoc2 = JsonUtil.parse(stringDoc2);
        } catch (Exception e) {
            throw JsonParserException.invalidArgInFunc(2, getFunctionNames()[0]);
        }

        return checkOverlap(jsonDoc1, jsonDoc2);
    }

    private boolean checkOverlap(Object target, Object candidate) {
        if (JsonUtil.isJsonArray(target)) {
            return arrayOverlaps((JSONArray) target, candidate);
        }
        if (JsonUtil.isJsonObject(target)) {
            return objOverlaps((JSONObject) target, candidate);
        }
        //target is a scalar
        if (JsonUtil.isJsonArray(candidate)) {
            return arrayOverlaps((JSONArray) candidate, target);
        }
        if (JsonUtil.isJsonObject(candidate)) {
            return objOverlaps((JSONObject) candidate, target);
        }
        return scalarEquals(target, candidate);
    }

    private boolean arrayOverlaps(JSONArray target, Object candidate) {
        //candidate is a jsonArray
        if (JsonUtil.isJsonArray(candidate)) {
            for (Object candElement : (JSONArray) candidate) {
                if (checkOverlap(target, candElement)) {
                    return true;
                }
            }
            return false;
        }
        //candidate is a scalar value
        for (Object element : target) {
            if (checkOverlap(element, candidate)) {
                return true;
            }
        }
        return false;
    }

    private boolean objOverlaps(JSONObject target, Object candidate) {
        //candidate must be json object
        if (JsonUtil.isJsonArray(candidate)) {
            for (Object canElement : (JSONArray) candidate) {
                if (checkOverlap(canElement, target)) {
                    return true;
                }
            }
            return false;
        } else if (JsonUtil.isJsonObject(candidate)) {
            if (target.isEmpty() && ((JSONObject) candidate).isEmpty()) {
                return true;
            }
            for (Map.Entry<String, Object> candEntry : ((JSONObject) candidate).entrySet()) {
                Object targetVal = target.get(candEntry.getKey());
                if (checkOverlap(targetVal, candEntry.getValue())) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    private boolean scalarEquals(Object target, Object candidate) {
        if (target instanceof Number && candidate instanceof Number) {
            target = NumberUtil.toBigDecimal((Number) target);
            candidate = NumberUtil.toBigDecimal((Number) candidate);
        }
        return Objects.equals(target, candidate);
    }
}
