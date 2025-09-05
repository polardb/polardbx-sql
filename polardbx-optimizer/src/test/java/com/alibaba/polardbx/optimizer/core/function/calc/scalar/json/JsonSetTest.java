package com.alibaba.polardbx.optimizer.core.function.calc.scalar.json;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import junit.framework.TestCase;
import org.junit.Assert;
import org.junit.Test;

import javax.xml.crypto.Data;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class JsonSetTest extends TestCase {

    @Test
    public void testCompute() {
        String json = "{\"a\":1,\"b\":2}";
        String path = "$.a";
        String val = "dasv";
        Slice slice = Slices.utf8Slice(val);
        List<DataType> operandTypes = Arrays.asList(null,null,null);
        JsonSet jsonSet = new JsonSet(operandTypes, DataTypes.StringType);
        Object object = jsonSet.compute(new Object[]{json, path, slice}, null);
        JSONObject jsonObject = JSON.parseObject(object.toString());
        Assert.assertEquals(jsonObject.getString("a"), val);
    }
}