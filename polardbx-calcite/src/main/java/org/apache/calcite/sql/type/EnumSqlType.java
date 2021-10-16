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

package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.util.SerializableCharset;

import java.util.Iterator;
import java.util.List;

/**
 * CalciteEnumType
 *
 * @author hongxi.chx
 */
public class EnumSqlType extends AbstractSqlType {
    //~ Static fields/initializers ---------------------------------------------

    private final RelDataTypeSystem typeSystem;
    private final List<String> values;
    private SqlCollation collation;
    private SerializableCharset wrappedCharset;

    /**
     * Constructs a type with values.
     *
     * @param typeName Type name
     */
    public EnumSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName,
                       List<String> values, SqlCollation collation, SerializableCharset serializableCharset) {
        super(typeName, true, null);
        this.typeSystem = typeSystem;
        this.typeName = typeName;
        this.values = values;
        this.collation = collation;
        this.wrappedCharset = serializableCharset;
        computeDigest();
    }

    // implement RelDataType
    @Override
    public SqlCollation getCollation() {
        return collation;
    }

    // implement RelDataTypeImpl
    @Override
    public void generateTypeString(StringBuilder sb, boolean withDetail) {
        // Called to make the digest, which equals() compares;
        // so equivalent data types must produce identical type strings.

        sb.append(typeName.name());

        if (values != null) {
            sb.append('(');
            final Iterator<String> iterator = values.iterator();
            boolean isFirst = true;
            while (iterator.hasNext()) {
                if (isFirst) {
                    isFirst = false;
                } else {
                    sb.append(", ");
                }
                final String next = iterator.next();
                sb.append(next);
            }
            sb.append(')');
        }
        if (!withDetail) {
            return;
        }
        if (wrappedCharset != null) {
            sb.append(" CHARACTER SET \"");
            sb.append(wrappedCharset.getCharset().name());
            sb.append("\"");
        }
        if (collation != null) {
            sb.append(" COLLATE \"");
            sb.append(collation.getCollationName());
            sb.append("\"");
        }
    }

    public List<String> getStringValues() {
        return values;
    }

}
