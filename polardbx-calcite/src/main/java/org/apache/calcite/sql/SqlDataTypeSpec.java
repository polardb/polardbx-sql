/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.sql;

import com.google.common.base.Preconditions;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.config.ConfigDataMode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeSystem;
import org.apache.calcite.sql.SqlWriter.FrameTypeEnum;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.EnumSqlType;
import org.apache.calcite.sql.type.SqlTypeFactoryImpl;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * Represents a SQL data type specification in a parse tree.
 *
 * <p>A <code>SqlDataTypeSpec</code> is immutable; once created, you cannot
 * change any of the fields.</p>
 *
 * <p>todo: This should really be a subtype of {@link SqlCall}.</p>
 *
 * <p>In its full glory, we will have to support complex type expressions
 * like:</p>
 *
 * <blockquote><code>ROW(<br>
 * NUMBER(5, 2) NOT NULL AS foo,<br>
 * ROW(BOOLEAN AS b, MyUDT NOT NULL AS i) AS rec)</code></blockquote>
 *
 * <p>Currently it only supports simple datatypes like CHAR, VARCHAR and DOUBLE,
 * with optional precision and scale.</p>
 */
public class SqlDataTypeSpec extends SqlNode {
    //~ Instance fields --------------------------------------------------------

    private final SqlIdentifier collectionsTypeName;
    private final SqlIdentifier typeName;
    private final int scale;
    private final int precision;
    private final String charSetName;
    private final String collationName;
    private final TimeZone timeZone;
    private final boolean unsigned;

    // BIT[(length)]
    // | TINYINT[(length)] [UNSIGNED] [ZEROFILL]
    // | SMALLINT[(length)] [UNSIGNED] [ZEROFILL]
    // | MEDIUMINT[(length)] [UNSIGNED] [ZEROFILL]
    // | INT[(length)] [UNSIGNED] [ZEROFILL]
    // | INTEGER[(length)] [UNSIGNED] [ZEROFILL]
    // | BIGINT[(length)] [UNSIGNED] [ZEROFILL]
    // | DOUBLE[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | REAL[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | FLOAT[(length,decimals)] [UNSIGNED] [ZEROFILL]
    // | DECIMAL[(length[,decimals])] [UNSIGNED] [ZEROFILL]
    // | NUMERIC[(length[,decimals])] [UNSIGNED] [ZEROFILL] 同上
    // | DATE
    // | TIME[(fsp)]
    // | TIMESTAMP[(fsp)]
    // | DATETIME[(fsp)]
    // | YEAR
    // | CHAR[(length)][CHARACTER SET charset_name] [COLLATE collation_name]
    // | VARCHAR(length)[CHARACTER SET charset_name] [COLLATE collation_name]
    // | BINARY[(length)]
    // | VARBINARY(length)
    // | TINYBLOB
    // | BLOB
    // | MEDIUMBLOB
    // | LONGBLOB
    // | TINYTEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | TEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | MEDIUMTEXT [BINARY][CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | LONGTEXT [BINARY][CHARACTER SET charset_name] [COLLATE collation_name]
    // | ENUM(value1,value2,value3,...)[CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | SET(value1,value2,value3,...)[CHARACTER SET charset_name] [COLLATE
    // collation_name]
    // | spatial_type 不支持
    private final boolean zerofill;
    /**
     * for text only
     */
    private final boolean binary;
    private final SqlLiteral length;
    private final SqlLiteral decimals;
    private final SqlLiteral charSet;
    private final SqlLiteral collation;
    /**
     * List<SqlLiteral>
     */
    private final SqlNodeList collectionVals;
    /**
     * fractional seconds precision
     */
    private final SqlLiteral fsp;
    private final boolean forDdl;
    /**
     * Whether data type is allows nulls.
     *
     * <p>Nullable is nullable! Null means "not specified". E.g.
     * {@code CAST(x AS INTEGER)} preserves has the same nullability as {@code x}.
     */
    private Boolean nullable;

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a type specification representing a regular, non-collection type.
     */
    public SqlDataTypeSpec(
        final SqlIdentifier typeName,
        int precision,
        int scale,
        String charSetName,
        TimeZone timeZone,
        SqlParserPos pos) {
        this(null, typeName, false, precision, scale, charSetName, timeZone, null, pos);
    }

    /**
     * Creates a type specification representing a collection type.
     */
    public SqlDataTypeSpec(
        SqlIdentifier collectionsTypeName,
        SqlIdentifier typeName,
        int precision,
        int scale,
        String charSetName,
        SqlParserPos pos) {
        this(collectionsTypeName, typeName, false, precision, scale, charSetName, null,
            null, pos);
    }

    public SqlDataTypeSpec(
        final SqlIdentifier typeName,
        boolean unsigned,
        int precision,
        int scale,
        String charSetName,
        TimeZone timeZone,
        SqlParserPos pos) {
        this(null, typeName, unsigned, precision, scale, charSetName, timeZone, null, pos);
    }

    /**
     * Creates a type specification.
     */
    public SqlDataTypeSpec(
        SqlIdentifier collectionsTypeName,
        SqlIdentifier typeName,
        boolean unsigned,
        int precision,
        int scale,
        String charSetName,
        TimeZone timeZone,
        Boolean nullable,
        SqlParserPos pos) {
        super(pos);
        this.collectionsTypeName = collectionsTypeName;
        this.typeName = typeName;
        this.precision = precision;
        this.scale = scale;
        this.charSetName = charSetName;
        this.collationName = null;
        this.timeZone = timeZone;
        this.nullable = nullable;

        this.unsigned = unsigned;
        this.zerofill = false;
        this.binary = false;
        this.length = null;
        this.decimals = null;
        this.charSet = null;
        this.collation = null;
        this.collectionVals = null;
        this.fsp = null;
        this.forDdl = false;
    }

    /**
     * Creates for DDL statement
     */

    public SqlDataTypeSpec(SqlParserPos pos, SqlIdentifier typeName, boolean unsigned, boolean zerofill, boolean binary,
                           SqlLiteral length, SqlLiteral decimals, SqlLiteral charSet,
                           SqlLiteral collation, SqlNodeList collectionVals, SqlLiteral fsp) {
        this(pos, typeName, unsigned, zerofill, binary, length, decimals, charSet, collation, collectionVals, fsp,
            true);
    }

    public SqlDataTypeSpec(SqlParserPos pos, SqlIdentifier typeName, boolean unsigned, boolean zerofill, boolean binary,
                           SqlLiteral length, SqlLiteral decimals, SqlLiteral charSet,
                           SqlLiteral collation, SqlNodeList collectionVals, SqlLiteral fsp, boolean nullable) {
        super(pos);

        this.collectionsTypeName = null;

        this.precision = null == length ? 0 : length.intValue(true);
        this.scale = null == decimals ? 0 : decimals.intValue(true);
        this.charSetName = null == charSet ? null : charSet.toValue();
        this.collationName = null == collation ? null : collation.toValue();
        this.timeZone = null;
        this.nullable = nullable;

        this.typeName = typeName;
        this.unsigned = unsigned;
        this.zerofill = zerofill;
        this.binary = binary;
        this.length = length;
        this.decimals = decimals;
        this.charSet = charSet;
        this.collation = collation;
        this.collectionVals = collectionVals;
        this.fsp = fsp;
        this.forDdl = true;
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public SqlNode clone(SqlParserPos pos) {
        if (forDdl) {
            return new SqlDataTypeSpec(getParserPosition(), typeName, unsigned, zerofill, binary, length, decimals,
                charSet,
                collation, collectionVals, fsp, nullable);
        } else {
            return (collectionsTypeName != null)
                ? new SqlDataTypeSpec(collectionsTypeName, typeName, precision, scale,
                charSetName, pos)
                : new SqlDataTypeSpec(typeName, precision, scale, charSetName, timeZone,
                pos);
        }
    }

    @Override
    public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
        return SqlMonotonicity.CONSTANT;
    }

    public SqlIdentifier getCollectionsTypeName() {
        return collectionsTypeName;
    }

    public SqlIdentifier getTypeName() {
        return typeName;
    }

    public int getScale() {
        return scale;
    }

    public int getPrecision() {
        return precision;
    }

    public String getCharSetName() {
        return charSetName;
    }

    public String getCollationName() {
        return collationName;
    }

    public TimeZone getTimeZone() {
        return timeZone;
    }

    public Boolean getNullable() {
        return nullable;
    }

    public SqlLiteral getLength() {
        return length;
    }

    /**
     * Returns a copy of this data type specification with a given
     * nullability.
     */
    public SqlDataTypeSpec withNullable(Boolean nullable) {
        if (Objects.equals(nullable, this.nullable)) {
            return this;
        }
        if (forDdl) {
            return new SqlDataTypeSpec(getParserPosition(),
                typeName,
                unsigned,
                zerofill,
                binary,
                length,
                decimals,
                charSet,
                collation,
                collectionVals,
                fsp,
                nullable);
        } else {
            return new SqlDataTypeSpec(collectionsTypeName, typeName, false, precision, scale,
                charSetName, timeZone, nullable, getParserPosition());
        }
    }

    /**
     * Returns a new SqlDataTypeSpec corresponding to the component type if the
     * type spec is a collections type spec.<br>
     * Collection types are <code>ARRAY</code> and <code>MULTISET</code>.
     */
    public SqlDataTypeSpec getComponentTypeSpec() {
        assert getCollectionsTypeName() != null;
        return new SqlDataTypeSpec(
            typeName,
            precision,
            scale,
            charSetName,
            timeZone,
            getParserPosition());
    }

    @Override
    public void unparse(
        SqlWriter writer,
        int leftPrec,
        int rightPrec) {
        String name = typeName.getSimple();

        if (forDdl) {
            writer.keyword(name);

            // add collection vals for ENUM type or Set type
            // e.g. ENUM('va1', 'val2', ...) or SET('var1', 'val2', ...)
            if ("ENUM".equalsIgnoreCase(name) || "SET".equalsIgnoreCase(name)) {
                if (null != collectionVals && collectionVals.size() > 0) {
                    final SqlWriter.Frame frame = writer.startList(FrameTypeEnum.FUN_CALL, "(", ")");
                    collectionVals.commaList(writer);
                    writer.endList(frame);

                    if (null != charSet) {
                        writer.keyword("CHARACTER SET");
                        writer.keyword(charSet.toValue());
                    }

                    if (null != collation) {
                        writer.keyword("COLLATE");
                        writer.keyword(collation.toValue());
                    }
                }
            } else {
                // add length vals for type
                // e.g. INT( length )
                if (null != this.length) {
                    final SqlWriter.Frame frame =
                        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                    this.length.unparse(writer, leftPrec, rightPrec);
                    if (null != this.decimals) {
                        writer.sep(",", true);
                        this.decimals.unparse(writer, leftPrec, rightPrec);
                    }
                    writer.endList(frame);
                }
                if (this.binary) {
                    writer.keyword("BINARY");
                }
                if (this.unsigned) {
                    writer.keyword("UNSIGNED");
                }
                if (this.zerofill) {
                    writer.keyword("ZEROFILL");
                }
                if (null != charSet) {
                    writer.keyword("CHARACTER SET");
                    writer.keyword(charSet.toValue());
                }
                if (null != collation) {
                    writer.keyword("COLLATE");
                    writer.keyword(collation.toValue());
                }
                if (null != fsp) {
                    final SqlWriter.Frame frame =
                        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                    this.fsp.unparse(writer, leftPrec, rightPrec);
                    writer.endList(frame);
                }
            }

            return;
        }

        if (SqlTypeName.get(name) != null) {
            SqlTypeName sqlTypeName = SqlTypeName.get(name);

            // we have a built-in data type
            writer.keyword(name);

            if (SqlTypeName.DATETIME_TYPES.contains(sqlTypeName)) {
                // for time / datetime / timestamp type
                if (sqlTypeName.allowsScale() && (scale >= 0)) {
                    final SqlWriter.Frame frame =
                        writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                    writer.print(scale);
                    writer.endList(frame);
                }
            } else if (sqlTypeName.allowsPrec() && (precision >= 0)) {
                final SqlWriter.Frame frame =
                    writer.startList(SqlWriter.FrameTypeEnum.FUN_CALL, "(", ")");
                writer.print(precision);
                if (sqlTypeName.allowsScale() && (scale >= 0)) {
                    writer.sep(",", true);
                    writer.print(scale);
                }
                writer.endList(frame);
            }

            if (charSetName != null) {
                writer.keyword("CHARACTER SET");
                writer.identifier(charSetName);
            }

            if (collectionsTypeName != null) {
                writer.keyword(collectionsTypeName.getSimple());
            }
        } else if (name.startsWith("_")) {
            // We're generating a type for an alien system. For example,
            // UNSIGNED is a built-in type in MySQL.
            // (Need a more elegant way than '_' of flagging this.)
            writer.keyword(name.substring(1));
        } else {
            // else we have a user defined type
            typeName.unparse(writer, leftPrec, rightPrec);
        }
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        validator.validateDataType(this);
    }

    @Override
    public <R> R accept(SqlVisitor<R> visitor) {
        return visitor.visit(this);
    }

    @Override
    public boolean equalsDeep(SqlNode node, Litmus litmus) {
        if (!(node instanceof SqlDataTypeSpec)) {
            return litmus.fail("{} != {}", this, node);
        }
        SqlDataTypeSpec that = (SqlDataTypeSpec) node;
        if (!SqlNode.equalDeep(
            this.collectionsTypeName,
            that.collectionsTypeName, litmus)) {
            return litmus.fail(null);
        }
        if (!this.typeName.equalsDeep(that.typeName, litmus)) {
            return litmus.fail(null);
        }
        if (this.precision != that.precision) {
            return litmus.fail("{} != {}", this, node);
        }
        if (this.scale != that.scale) {
            return litmus.fail("{} != {}", this, node);
        }
        if (!Objects.equals(this.timeZone, that.timeZone)) {
            return litmus.fail("{} != {}", this, node);
        }
        if (!Objects.equals(this.charSetName, that.charSetName)) {
            return litmus.fail("{} != {}", this, node);
        }
        return litmus.succeed();
    }

    /**
     * Throws an error if the type is not built-in.
     */
    public RelDataType deriveType(SqlValidator validator) {
        String name = typeName.getSimple();

        // for now we only support builtin datatypes
        DrdsTypeName drdsTypeName = DrdsTypeName.from(name);
        if (drdsTypeName == null) {
            throw validator.newValidationError(this,
                RESOURCE.unknownDatatypeName(name));
        }

        SqlTypeName sqlTypeName;
        if (!unsigned) {
            sqlTypeName = drdsTypeName.getCalciteTypeName();
        } else {
            sqlTypeName = drdsTypeName.getUnsignedCalciteTypeName();
        }

        if (sqlTypeName == null) {
            throw validator.newValidationError(this,
                RESOURCE.unknownDatatypeName(name));
        }

        if (null != collectionsTypeName) {
            final String collectionName = collectionsTypeName.getSimple();
            if (SqlTypeName.get(collectionName) == null) {
                throw validator.newValidationError(this,
                    RESOURCE.unknownDatatypeName(collectionName));
            }
        }

        RelDataTypeFactory typeFactory = validator.getTypeFactory();
        return deriveType(typeFactory);
    }

    /**
     * Does not throw an error if the type is not built-in.
     */
    public RelDataType deriveType(RelDataTypeFactory typeFactory) {
        return deriveType(typeFactory, false);
    }

    /**
     * Converts this type specification to a {@link RelDataType}.
     *
     * <p>Does not throw an error if the type is not built-in.
     *
     * @param nullable Whether the type is nullable if the type specification
     * does not explicitly state
     */
    public RelDataType deriveType(RelDataTypeFactory typeFactory,
                                  boolean nullable) {
        RelDataTypeSystem typeSystem = typeFactory.getTypeSystem();
        final String name = typeName.getSimple().toUpperCase();

        DrdsTypeName drdsTypeName = DrdsTypeName.from(name);
        if (drdsTypeName == null) {
            throw new IllegalArgumentException("Datatype '" + name + "' not supported");
        }

        SqlTypeName sqlTypeName;
        if (!unsigned) {
            sqlTypeName = drdsTypeName.getCalciteTypeName();
        } else {
            sqlTypeName = drdsTypeName.getUnsignedCalciteTypeName();
        }

        // NOTE eric.fy: some datatypes such as INT(11) contains length argument but
        // actually meaningless. Here we remove such arguments
        int precision = sqlTypeName.allowsPrec() ? this.precision : RelDataType.PRECISION_NOT_SPECIFIED;
        int scale = sqlTypeName.allowsScale() ? this.scale : RelDataType.SCALE_NOT_SPECIFIED;

        if (sqlTypeName == SqlTypeName.VARCHAR) {
            if (this.getLength() != null) {
                precision = this.getLength().intValue(false);
            }
        }

        // For time/datetime/timestamp types
        if (SqlTypeFamily.DATETIME.getTypeNames().contains(sqlTypeName) && sqlTypeName != SqlTypeName.DATE) {
            // fix scale and precision of datetime type.
            scale = fsp == null
                ? (this.scale > typeSystem.getMaxScale(sqlTypeName) ? 0 : this.scale)
                : fsp.intValue(false);
            precision = precision >= 0 ? precision : typeSystem.getMaxPrecision(sqlTypeName);
        }

        // NOTE jvs 15-Jan-2009:  earlier validation is supposed to
        // have caught these, which is why it's OK for them
        // to be assertions rather than user-level exceptions.
        RelDataType type;
        if (sqlTypeName == SqlTypeName.ENUM) {
            List<String> list = new ArrayList();
            for (SqlNode sqlNode : this.collectionVals.getList()) {
                assert sqlNode instanceof SqlLiteral;
                final String stringValue = ((SqlLiteral) sqlNode).getStringValue();
                list.add(stringValue);
            }
            RelDataType newType = new EnumSqlType(typeFactory.getTypeSystem(), SqlTypeName.ENUM, list, null, null);
            type = ((SqlTypeFactoryImpl) typeFactory).canonize(newType);
        } else if ((precision >= 0) && (scale >= 0)) {
            assert sqlTypeName.allowsPrecScale(true, true);
            type = typeFactory.createSqlType(sqlTypeName, precision, scale);
        } else if (precision >= 0) {
            assert sqlTypeName.allowsPrecNoScale();
            type = typeFactory.createSqlType(sqlTypeName, precision);
        } else if (precision == -1 && sqlTypeName.allowsPrec()) { // for sqltype not specified precision
            type = typeFactory.createSqlType(sqlTypeName);
        } else {
            assert sqlTypeName.allowsNoPrecNoScale();
            type = typeFactory.createSqlType(sqlTypeName);
        }

        if (SqlTypeUtil.inCharFamily(type)) {
            // Applying Syntax rule 10 from SQL:99 spec section 6.22 "If TD is a
            // fixed-length, variable-length or large object character string,
            // then the collating sequence of the result of the <cast
            // specification> is the default collating sequence for the
            // character repertoire of TD and the result of the <cast
            // specification> has the Coercible coercibility characteristic."
            SqlCollation sqlCollation = null;

            Charset sqlCharset;
            if (null == charSetName || ConfigDataMode.isFastMock()) {
                sqlCharset = typeFactory.getDefaultCharset();
            } else {
                sqlCharset = Optional.of(charSetName)
                    .map(CharsetName::convertStrToJavaCharset)
                    .orElseGet(
                        () -> CharsetName.defaultCharset().toJavaCharset()
                    );
            }
            sqlCollation = new SqlCollation(sqlCharset,
                collationName,
                SqlCollation.Coercibility.IMPLICIT);

            type =
                typeFactory.createTypeWithCharsetAndCollation(
                    type,
                    sqlCharset,
                    sqlCollation);
        }

        if (null != collectionsTypeName) {
            final String collectionName = collectionsTypeName.getSimple();
            final SqlTypeName collectionsSqlTypeName =
                Preconditions.checkNotNull(SqlTypeName.get(collectionName),
                    collectionName);

            switch (collectionsSqlTypeName) {
            case MULTISET:
                type = typeFactory.createMultisetType(type, -1);
                break;

            default:
                throw Util.unexpected(collectionsSqlTypeName);
            }
        }

        // TODO: nullable not supported yet
        //if (this.nullable != null) {
        //  nullable = this.nullable;
        //}
        type = typeFactory.createTypeWithNullability(type, nullable);

        return type;
    }

    public enum DrdsTypeName {
        BIT(SqlTypeName.BIT),
        TINYINT(SqlTypeName.TINYINT, SqlTypeName.TINYINT_UNSIGNED),
        SMALLINT(SqlTypeName.SMALLINT, SqlTypeName.SMALLINT_UNSIGNED),
        MEDIUMINT(SqlTypeName.MEDIUMINT, SqlTypeName.MEDIUMINT_UNSIGNED),
        INTEGER(SqlTypeName.INTEGER, SqlTypeName.INTEGER_UNSIGNED),
        BIGINT(SqlTypeName.BIGINT, SqlTypeName.BIGINT_UNSIGNED),
        REAL(SqlTypeName.REAL),
        DOUBLE(SqlTypeName.DOUBLE),
        FLOAT(SqlTypeName.FLOAT),
        DECIMAL(SqlTypeName.DECIMAL),
        DATE(SqlTypeName.DATE),
        TIME(SqlTypeName.TIME),
        TIMESTAMP(SqlTypeName.TIMESTAMP),
        DATETIME(SqlTypeName.DATETIME),
        YEAR(SqlTypeName.YEAR),
        CHAR(SqlTypeName.CHAR),
        VARCHAR(SqlTypeName.VARCHAR),
        BINARY(SqlTypeName.BINARY),
        VARBINARY(SqlTypeName.VARBINARY),
        TINYBLOB(SqlTypeName.BLOB),
        BLOB(SqlTypeName.BLOB),
        MEDIUMBLOB(SqlTypeName.BLOB),
        LONGBLOB(SqlTypeName.BLOB),
        TINYTEXT(SqlTypeName.VARCHAR),
        TEXT(SqlTypeName.VARCHAR),
        MEDIUMTEXT(SqlTypeName.VARCHAR),
        LONGTEXT(SqlTypeName.VARCHAR),
        ENUM(SqlTypeName.ENUM),
        SET(SqlTypeName.VARCHAR),
        GEOMETRY(SqlTypeName.GEOMETRY),
        POINT(SqlTypeName.BINARY),
        LINESTRING(SqlTypeName.BINARY),
        POLYGON(SqlTypeName.BINARY),
        MULTIPOINT(SqlTypeName.BINARY),
        MULTILINESTRING(SqlTypeName.BINARY),
        MULTIPOLYGON(SqlTypeName.BINARY),
        GEOMETRYCOLLECTION(SqlTypeName.BINARY),
        NVARCHAR(SqlTypeName.VARCHAR),
        NCHAR(SqlTypeName.CHAR),
        JSON(SqlTypeName.JSON),
        BOOLEAN(SqlTypeName.BOOLEAN),

        UNSIGNED(SqlTypeName.UNSIGNED), // only for CAST
        SIGNED(SqlTypeName.SIGNED), // only for CAST
        ;

        public static final EnumSet TYPE_WITH_LENGTH =
            EnumSet.of(TINYINT, SMALLINT, MEDIUMINT, INTEGER, BIGINT, DOUBLE, REAL, FLOAT, DECIMAL);

        public static final EnumSet TYPE_WITH_LENGTH_DECIMALS =
            EnumSet.of(DOUBLE, REAL, FLOAT, DECIMAL);

        public static final EnumSet TYPE_WITH_FSP =
            EnumSet.of(TIME, TIMESTAMP, DATETIME);

        /**
         * CHAR, VARCHAR, VARBINARY, BINARY
         */
        public static final EnumSet TYPE_WITH_LENGTH_STRING =
            EnumSet.of(CHAR, VARCHAR, VARBINARY, BINARY);

        private final SqlTypeName calciteTypeName;
        private final SqlTypeName unsignedCalciteTypeName;

        DrdsTypeName(SqlTypeName calciteTypeName) {
            this(calciteTypeName, calciteTypeName);
        }

        DrdsTypeName(SqlTypeName calciteTypeName, SqlTypeName unsignedCalciteTypeName) {
            this.calciteTypeName = calciteTypeName;
            this.unsignedCalciteTypeName = unsignedCalciteTypeName;
        }

        public static DrdsTypeName from(String value) {
            // type alias here
            switch (value.toUpperCase()) {
            case "INT":
                return INTEGER;
            case "NUMERIC":
            case "DEC":
                return DECIMAL;
            case "GEOMCOLLECTION":
                return GEOMETRYCOLLECTION;
            case "UNSIGNED INT":
            case "UNSIGNED INTEGER":
                // UNSIGNED [INTEGER]
                // Produces an unsigned integer value.
                return UNSIGNED;
            case "SIGNED INT":
            case "SIGNED INTEGER":
                // SIGNED [INTEGER]
                // Produces a signed integer value.
                return SIGNED;
            default:
                return DrdsTypeName.valueOf(value);
            }
        }

        public SqlTypeName getCalciteTypeName() {
            return calciteTypeName;
        }

        /**
         * @return unsigned calcite type name, or the original data type if not applicable
         */
        public SqlTypeName getUnsignedCalciteTypeName() {
            return unsignedCalciteTypeName;
        }

        public boolean isA(EnumSet enumSet) {
            return enumSet.contains(this);
        }
    }
}

// End SqlDataTypeSpec.java
