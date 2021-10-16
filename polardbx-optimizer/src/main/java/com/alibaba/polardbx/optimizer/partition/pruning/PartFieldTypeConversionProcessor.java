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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.field.TypeConversionStatus;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PredicateBoolean;
import com.alibaba.polardbx.optimizer.partition.exception.InvalidTypeConversionException;

import java.util.HashMap;
import java.util.Map;

/**
 * @author chenghui.lch
 */
public class PartFieldTypeConversionProcessor {

    protected enum ActionType {
        IGNORE,
        REJECT
    }

    protected static class TypeConversionStatusAction {
        protected Map<PartFieldAccessType, ActionType> actionInfos;

        protected TypeConversionStatusAction(Map<PartFieldAccessType, ActionType> actionInfos) {
            this.actionInfos = actionInfos;
        }
    }

    protected static Map<TypeConversionStatus, TypeConversionStatusAction> statusActionMaps = new HashMap<>();

    static {

        Map<PartFieldAccessType, ActionType> actionInfos = null;
        TypeConversionStatusAction action = null;
        TypeConversionStatus status = null;

        /**
         * TYPE_OK
         */
        status = TypeConversionStatus.TYPE_OK;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_NOTE_TIME_TRUNCATED
         */
        status = TypeConversionStatus.TYPE_NOTE_TIME_TRUNCATED;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_WARN_OUT_OF_RANGE
         */
        status = TypeConversionStatus.TYPE_WARN_OUT_OF_RANGE;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_NOTE_TRUNCATED
         */
        status = TypeConversionStatus.TYPE_NOTE_TRUNCATED;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_WARN_TRUNCATED
         */
        status = TypeConversionStatus.TYPE_WARN_TRUNCATED;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_WARN_INVALID_STRING
         */
        status = TypeConversionStatus.TYPE_WARN_INVALID_STRING;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_ERR_NULL_CONSTRAINT_VIOLATION
         */
        status = TypeConversionStatus.TYPE_ERR_NULL_CONSTRAINT_VIOLATION;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.IGNORE);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_ERR_BAD_VALUE
         */
        status = TypeConversionStatus.TYPE_ERR_BAD_VALUE;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.REJECT);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_ERR_OOM
         */
        status = TypeConversionStatus.TYPE_ERR_OOM;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.IGNORE);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.REJECT);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

        /**
         * TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST
         */
        status = TypeConversionStatus.TYPE_ERR_UNSUPPORTED_IMPLICIT_CAST;
        actionInfos = new HashMap<>();
        actionInfos.put(PartFieldAccessType.QUERY_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DML_PRUNING, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.DDL_EXECUTION, ActionType.REJECT);
        actionInfos.put(PartFieldAccessType.META_LOADING, ActionType.REJECT);
        action = new TypeConversionStatusAction(actionInfos);
        statusActionMaps.put(status, action);

    }

    public static void processTypeConversionStatus(PartFieldAccessType accessType,
                                                   DataType srcDataType, PartitionField storedField,
                                                   boolean[] endpoints) {

        TypeConversionStatus status = storedField.lastStatus();
        DataType tarDataType = storedField.dataType();
        PredicateBoolean predBool = storedField.lastPredicateBoolean();

        if (accessType == PartFieldAccessType.QUERY_PRUNING
            && predBool != PredicateBoolean.IS_NOT_ALWAYS_TRUE_OR_FALSE && endpoints != null) {
            // Do Full Scan by throwing InvalidTypeConversionException
            throw new InvalidTypeConversionException(accessType, status, predBool, srcDataType, tarDataType);
        }

        TypeConversionStatusAction action = statusActionMaps.get(status);
        ActionType type = action.actionInfos.get(accessType);
        if (type == ActionType.IGNORE) {
            return;
        }
        if (type == ActionType.REJECT) {
            throw new InvalidTypeConversionException(accessType, status, predBool, srcDataType, tarDataType);
        }
    }
}
