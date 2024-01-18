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

package com.alibaba.polardbx.optimizer.core.Xplan;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.Xplanner.XPlanUtil;
import com.alibaba.polardbx.rpc.XUtil;
import com.google.protobuf.ByteString;
import com.googlecode.protobuf.format.JsonFormat;
import com.mysql.cj.x.protobuf.PolarxExecPlan;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.type.SqlTypeName;

import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Map;

/**
 * @version 1.0
 */
public class XPlanTemplate implements IXPlan {

    private final PolarxExecPlan.AnyPlan template;
    private final ByteString digest;
    private final List<SqlIdentifier> tableNames;
    private final List<XPlanUtil.ScalarParamInfo> paramInfos;

    public XPlanTemplate(PolarxExecPlan.AnyPlan template, List<SqlIdentifier> tableNames,
                         List<XPlanUtil.ScalarParamInfo> paramInfos) {
        this.template = template;
        ByteString digest;
        try {
            digest = ByteString.copyFrom(MessageDigest.getInstance("md5").digest(template.toByteArray()));
        } catch (NoSuchAlgorithmException e) {
            digest = null;
        }
        this.digest = digest;
        this.tableNames = tableNames;
        this.paramInfos = paramInfos;
    }

    public PolarxExecPlan.AnyPlan getTemplate() {
        return template;
    }

    public List<SqlIdentifier> getTableNames() {
        return tableNames;
    }

    public List<XPlanUtil.ScalarParamInfo> getParamInfos() {
        return paramInfos;
    }

    @Override
    public PolarxExecPlan.ExecPlan.Builder getXPlan(String dbIndex, List<String> phyTableNames,
                                                    Map<Integer, ParameterContext> params,
                                                    ExecutionContext executionContext) {
        final PolarxExecPlan.ExecPlan.Builder builder = PolarxExecPlan.ExecPlan.newBuilder();

        builder.setPlan(template);
        builder.setPlanDigest(digest);
        for (XPlanUtil.ScalarParamInfo info : paramInfos) {
            switch (info.getType()) {
            case DynamicParam: {
                final Object val = params.get(info.getId() + 1).getValue();
                if (!info.isNullable() && null == val) {
                    return null; // Forbidden. This happens when use GetPlan and SQL is xx=? and parameter is null.
                }
                // Forbid binary/decimal/Date compare with plan(collation/precision/format may mismatch).
                if (val instanceof byte[] || val instanceof BigDecimal || val instanceof java.util.Date) {
                    return null;
                }
                // NOTE: When compare value with BIT type, uint must be used.
                if (info.getDataType().getSqlTypeName() == SqlTypeName.BIT && val instanceof byte[]) {
                    final ByteBuffer buf =
                        ByteBuffer.allocate(Long.BYTES).put((byte[]) val)
                            .order(ByteOrder.LITTLE_ENDIAN);
                    buf.rewind();
                    builder.addParameters(XUtil.genUIntScalar(buf.getLong(0)));
                } else {
                    builder.addParameters(XUtil.genScalar(val, null));
                }
            }
            break;
            case TableName:
                builder.addParameters(XUtil.genIdentifierScalarStringCompatible(phyTableNames.get(info.getId())));
                break;
            case SchemaName:
                builder.addParameters(XUtil.genIdentifierScalarStringCompatible(dbIndex));
                break;
            }
        }
        return builder;
    }

    public PolarxExecPlan.ExecPlan explain() {
        return explain(null);
    }

    public PolarxExecPlan.ExecPlan explain(ExecutionContext ec) {
        final Map<Integer, ParameterContext> params =
            null == ec || null == ec.getParams() ? null : ec.getParams().getCurrentParameter();
        final PolarxExecPlan.ExecPlan.Builder builder = PolarxExecPlan.ExecPlan.newBuilder();

        builder.setPlan(template);
        for (XPlanUtil.ScalarParamInfo info : paramInfos) {
            switch (info.getType()) {
            case DynamicParam:
                if (null == params) {
                    builder.addParameters(XUtil.genUtf8StringScalar("?" + info.getId()));
                } else {
                    if (!info.isNullable() && null == params.get(info.getId() + 1).getValue()) {
                        return null; // Forbidden. This happens when use GetPlan and SQL is xx=? and parameter is null.
                    }
                    // Forbid binary/decimal/Date compare with plan(collation/precision/format may mismatch).
                    final Object val = params.get(info.getId() + 1).getValue();
                    if (val instanceof byte[] || val instanceof BigDecimal || val instanceof java.util.Date) {
                        return null;
                    }
                    // NOTE: When compare value with BIT type, uint must be used.
                    if (info.getDataType().getSqlTypeName() == SqlTypeName.BIT && val instanceof byte[]) {
                        final ByteBuffer buf =
                            ByteBuffer.allocate(Long.BYTES).put((byte[]) val)
                                .order(ByteOrder.LITTLE_ENDIAN);
                        buf.rewind();
                        builder.addParameters(XUtil.genUIntScalar(buf.getLong(0)));
                    } else {
                        builder.addParameters(XUtil.genScalar(val, null));
                    }
                }
                break;
            case TableName:
                builder.addParameters(XUtil.genUtf8StringScalar(tableNames.get(info.getId()).getLastName()));
                break;
            case SchemaName:
                builder.addParameters(XUtil.genUtf8StringScalar(
                    2 == tableNames.get(info.getId()).names.size() ?
                        tableNames.get(info.getId()).names.get(0) :
                        ""));
                break;
            }
        }
        return builder.build();
    }

    @Override
    public String toString() {
        final JsonFormat format = new JsonFormat();
        return format.printToString(explain());
    }
}
