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

package com.alibaba.polardbx.optimizer.planmanager;

import com.alibaba.polardbx.druid.sql.parser.ByteString;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by simiao.zw on 2014/7/29. To store each PREPARE SQL and its status
 * for COM_STMT_PREPARE and String for COM_QUERY no need to add state for
 * statement, since new statement only if prepared ok and execute will fail if
 * it cannot find proper statement by stmt_id. and any parsing error will cause
 * the current sql fail, no need to keep the last statement status for the next
 * statement instruction.
 */
public class Statement {

    private String stmt_id;
    private ByteString rawSql;

    /**
     * <pre>
     * stmt_paramTypes store the ?BindVal related datatypes, this is for COM_QUERY's prepare
     * and COM_STMT_PREPARE as well
     *
     * IMPORTANT!!!
     * parameters type is NOT the column type. so for select ? as a, the parameter type
     * should be String, no matter what the real column type it is. the response will handle the rowset
     * for it's column type, they are independent. created by tddl5 parser and used only when
     * transfer incoming java object to DataType and fill ParameterContext before execute SQL.
     * so here might check if the incoming java object can be transferred to ParameterContext required params.
     *
     * Since COM_QUERY version of prepare statement has no exact params types define, so we use only String as
     * DataType here, and for those unknown types of columns we use DataType.String which is exact match the COM_QUERY
     * String based requirement.
     *
     * For COM_STMT_PREPARE, since execute will know the exact value type, so if Column datatype is undefined, use packet type
     * instead.
     * </pre>
     */
    private int prepareParamCount;

    /**
     * <pre>
     * will only be used on COM_STMT_xxx and updated by EXECUTE & SEND_LONG_DATA
     * might be different size as stmt_num_params, since stmt_num_params reflect to the ?
     * of BindVal in sql, and params reflect to the real params.
     *
     * paramTypes is mysql raw type
     *
     * Since client will send blob data before execute, so we must save it in advance, furthermore,
     * since we don't know the exact value length(maybe not equal to DataTypes in prepare) so we can't
     * create array in phase execute or send_long_data, so we ust a hashmap instead rather than array
     * the key is paramIndex
     *
     * IMPORTANT!!!
     * paramTypes is parameters type, NOT the column type. so for select ? as a, the paramTypes
     * should be String, no matter what the real column type it is. the response will handle the rowset
     * for it's column type, they are independent. This type is read from incoming packet and only used when
     * parsing incoming java Object.
     * </pre>
     */

    private Map<Integer, Short> paramTypes = new HashMap<Integer, Short>();

    /**
     * <pre>
     * The executeLock used between execute and send_long_data, since we should not sync in
     * whole ServerPrepareStatementHandler for performance issue, so we should introduce another
     * lock to keep send_data_long always finished before execute. send_data_long has no response
     * to client.
     * </pre>
     */
    private final ReentrantLock executeLock = new ReentrantLock();

    private boolean isSetQuery;

    /**
     * lazy init params
     * content of longDataParams is carried in send_long_data
     */
    private Map<Integer, Object> longDataParams;
    private List<Object> paramArray;

    public Statement(String stmt_id, ByteString rawSql) {
        this.stmt_id = stmt_id;
        this.rawSql = rawSql;
    }

    public String getStmtId() {
        return stmt_id;
    }

    public ByteString getRawSql() {
        return rawSql;
    }

    public List<Object> getParams() {
        return paramArray;
    }

    public Object getParam(int paramIndex) {
        return paramArray.get(paramIndex);
    }

    // can only set one param rather the whole map
    public synchronized void setParam(int paramIndex, Object value) {
        paramArray.set(paramIndex, value);
    }

    public void initLongDataParams() {
        longDataParams = new HashMap<>();
    }

    public void setLongDataParam(int paramIndex, Object value) {
        longDataParams.put(paramIndex, value);
    }

    public Object getLongDataParam(int paramIndex) {
        return longDataParams.get(paramIndex);
    }

    public synchronized Map<Integer, Object> getLongDataParams() {
        return longDataParams;
    }

    public synchronized void clearLongDataParams() {
        if (longDataParams != null) {
            longDataParams.clear();
        }
    }

    public void clearParamTypes() {
        paramTypes.clear();
    }

    public void clearParams() {
        if (longDataParams != null) {
            longDataParams.clear();
        }
        if (paramArray != null) {
            paramArray.clear();
        }
    }

    public Map<Integer, Short> getParamTypes() {
        return paramTypes;
    }

    public short getParamType(int paramIndex) {
        return paramTypes.get(paramIndex);
    }

    public void putAllParamTypes(Map<Integer, Short> intypes) {
        paramTypes.putAll(intypes);
    }

    public void setParamType(int paramIndex, short paramType) {
        paramTypes.put(paramIndex, paramType);
    }

    public int getPrepareParamCount() {
        return prepareParamCount;
    }

    public void setPrepareParamCount(int prepareParamCount) {
        this.prepareParamCount = prepareParamCount;
    }

    public boolean isSetQuery() {
        return isSetQuery;
    }

    public void setSetQuery(boolean setQuery) {
        isSetQuery = setQuery;
    }

    public List<Object> getParamArray() {
        return paramArray;
    }

    public void setParamArray(List<Object> paramArray) {
        this.paramArray = paramArray;
    }

    public static Object processStringValue(Object obj) {
        Object ret = obj;

        if (obj instanceof String && ((String) obj).length() > 0) {
            String str = (String) obj;
            ret = trimStringValue(str);
        }

        return ret;
    }

    public static String trimStringValue(String str) {
        String ret = str;
        if ((str.charAt(0) == '"' && str.charAt(str.length() - 1) == '"')
            || (str.charAt(0) == '\'' && str.charAt(str.length() - 1) == '\'')) {
            ret = str.substring(1, str.length() - 1);
        }

        return ret;
    }

    public Lock getExeLock() {
        return executeLock;
    }
}
