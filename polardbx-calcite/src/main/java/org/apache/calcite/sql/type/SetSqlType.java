package org.apache.calcite.sql.type;

import org.apache.calcite.rel.type.RelDataTypeSystem;

import java.util.List;

/**
 * This class is not fully implemented yet.
 * This can be used while converting from jdbc type to Polardb-X type, and cannot be used directly in optimizer
 */
public class SetSqlType extends BasicSqlType {
    private final List<String> setValues;

    public SetSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName, List<String> setValues) {
        super(typeSystem, typeName);
        this.setValues = setValues;
    }

    public SetSqlType(RelDataTypeSystem typeSystem, SqlTypeName typeName, int precision, List<String> setValues) {
        super(typeSystem, typeName, precision);
        this.setValues = setValues;
    }

    public List<String> getSetValues() {
        return setValues;
    }

}
