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

package org.apache.calcite.sql.validate;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;

import java.util.List;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlPartitionByScope extends ListScope {
    private final SqlNode sqlPartitionBy;

    public SqlPartitionByScope(SqlValidatorScope parent, SqlNode sqlNode) {
        super(parent);
        this.sqlPartitionBy = sqlNode;
    }

    @Override
    public SqlNode getNode() {
        return sqlPartitionBy;
    }

    @Override
    public SqlQualified fullyQualify(SqlIdentifier identifier) {
        String name = identifier.names.get(0);
        SqlNode createTbNode = this.getParent().getNode();
        SqlValidatorNamespace createTbNs = validator.getNamespace(createTbNode);
        RelDataType tblFldDataTypes = validator.deriveType(this.getParent(),createTbNode);
        assert tblFldDataTypes.isStruct() == true;
        RelDataTypeField fld = null;
        List<RelDataTypeField> tblFldList = tblFldDataTypes.getFieldList();
        for (int i = 0; i < tblFldList.size(); i++) {
            RelDataTypeField dtFld = tblFldList.get(i);
            String fldColName = dtFld.getName();
            if (name.equalsIgnoreCase(fldColName)) {
                fld = dtFld;
                break;
            }
        }
        if (fld != null) {
            return SqlQualified.create(this, 1, createTbNs, identifier);
        }
        return super.fullyQualify(identifier);
    }

    @Override
    public RelDataType resolveColumn(String columnName, SqlNode ctx) {
        SqlCreateTable sqlCreateTable = (SqlCreateTable) this.parent.getNode();
        RelDataType tblFldDataTypes = this.validator.deriveType(this.parent, sqlCreateTable);
        assert tblFldDataTypes.isStruct() == true;
        List<RelDataTypeField> tblFldList = tblFldDataTypes.getFieldList();
        RelDataType tarFldDataType = null;
        for (int i = 0; i < tblFldList.size(); i++) {
            RelDataTypeField dtFld = tblFldList.get(i);
            String fldColName = dtFld.getName();
            if (columnName.equalsIgnoreCase(fldColName)) {
                tarFldDataType = dtFld.getType();
                break;
            }
        }
        return tarFldDataType;
    }
}
