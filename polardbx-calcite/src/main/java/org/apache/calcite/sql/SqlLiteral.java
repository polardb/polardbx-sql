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

import com.alibaba.polardbx.common.datatype.UInt64;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.fun.SqlLiteralChainOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.sql.util.SqlVisitor;
import org.apache.calcite.sql.validate.SqlMonotonicity;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;
import org.apache.calcite.util.BitString;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.EqualsContext;
import org.apache.calcite.util.IntervalString;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.NlsString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.calcite.util.Util;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.charset.UnsupportedCharsetException;
import java.util.Calendar;
import java.util.Objects;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <code>SqlLiteral</code> is a constant. It is, appropriately, immutable.
 *
 * <p>How is the value stored? In that respect, the class is somewhat of a black
 * box. There is a {@link #getValue} method which returns the value as an
 * object, but the type of that value is implementation detail, and it is best
 * that your code does not depend upon that knowledge. It is better to use
 * task-oriented methods such as {@link #toSqlString(SqlDialect)} and
 * {@link #toValue}.</p>
 *
 * <p>If you really need to access the value directly, you should switch on the
 * value of the {@link #typeName} field, rather than making assumptions about
 * the runtime type of the {@link #value}.</p>
 *
 * <p>The allowable types and combinations are:
 *
 * <table>
 * <caption>Allowable types for SqlLiteral</caption>
 * <tr>
 * <th>TypeName</th>
 * <th>Meaing</th>
 * <th>Value type</th>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#NULL}</td>
 * <td>The null value. It has its own special type.</td>
 * <td>null</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BOOLEAN}</td>
 * <td>Boolean, namely <code>TRUE</code>, <code>FALSE</code> or <code>
 * UNKNOWN</code>.</td>
 * <td>{@link Boolean}, or null represents the UNKNOWN value</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DECIMAL}</td>
 * <td>Exact number, for example <code>0</code>, <code>-.5</code>, <code>
 * 12345</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DOUBLE}</td>
 * <td>Approximate number, for example <code>6.023E-23</code>.</td>
 * <td>{@link BigDecimal}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#DATE}</td>
 * <td>Date, for example <code>DATE '1969-04'29'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIME}</td>
 * <td>Time, for example <code>TIME '18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#TIMESTAMP}</td>
 * <td>Timestamp, for example <code>TIMESTAMP '1969-04-29
 * 18:37:42.567'</code></td>
 * <td>{@link Calendar}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#CHAR}</td>
 * <td>Character constant, for example <code>'Hello, world!'</code>, <code>
 * ''</code>, <code>_N'Bonjour'</code>, <code>_ISO-8859-1'It''s superman!'
 * COLLATE SHIFT_JIS$ja_JP$2</code>. These are always CHAR, never VARCHAR.</td>
 * <td>{@link NlsString}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#BINARY}</td>
 * <td>Binary constant, for example <code>X'ABC'</code>, <code>X'7F'</code>.
 * Note that strings with an odd number of hexits will later become values of
 * the BIT datatype, because they have an incomplete number of bytes. But here,
 * they are all binary constants, because that's how they were written. These
 * constants are always BINARY, never VARBINARY.</td>
 * <td>{@link BitString}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#SYMBOL}</td>
 * <td>A symbol is a special type used to make parsing easier; it is not part of
 * the SQL standard, and is not exposed to end-users. It is used to hold a
 * symbol, such as the LEADING flag in a call to the function <code>
 * TRIM([LEADING|TRAILING|BOTH] chars FROM string)</code>.</td>
 * <td>An {@link Enum}</td>
 * </tr>
 * <tr>
 * <td>{@link SqlTypeName#INTERVAL_YEAR}
 *     .. {@link SqlTypeName#INTERVAL_SECOND}</td>
 * <td>Interval, for example <code>INTERVAL '1:34' HOUR</code>.</td>
 * <td>{@link SqlIntervalLiteral.IntervalValue}.</td>
 * </tr>
 * </table>
 */
public class SqlLiteral extends SqlNode {
  //~ Instance fields --------------------------------------------------------
  /**
   * <p>
   *     In a {@link SqlNodeList}, the field {@link SqlLiteral#typeName}
   *     usually shares the same SqlTypeName object.
   * </p>
   */
  private static final long INTERNAL_OBJECT_SIZE_COUNT_ONCE = SqlTypeName.getSize();

  /**
   * The type with which this literal was declared. This type is very
   * approximate: the literal may have a different type once validated. For
   * example, all numeric literals have a type name of
   * {@link SqlTypeName#DECIMAL}, but on validation may become
   * {@link SqlTypeName#INTEGER}.
   */
  private final SqlTypeName typeName;

  /**
   * The value of this literal. The type of the value must be appropriate for
   * the typeName, as defined by the {@link #valueMatchesType} method.
   * <p>
   *     The actual size of value cannot be obtained,
   *     using an estimated mean value if necessary.
   * </p>
   * <p>
   *     If the overhead of computing in runtime is low,
   *     the size can also be determined by {@link SqlLiteral#typeName}.
   * </p>
   */
  protected final Object value;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a <code>SqlLiteral</code>.
   */
  protected SqlLiteral(
      Object value,
      SqlTypeName typeName,
      SqlParserPos pos) {
    super(pos);
    this.value = value;
    this.typeName = typeName;
    assert typeName != null;
    assert valueMatchesType(value, typeName);
  }

  //~ Methods ----------------------------------------------------------------
  @Override
  public long getCountOnceObjectSize() {
    return super.getCountOnceObjectSize() + INTERNAL_OBJECT_SIZE_COUNT_ONCE;
  }

  public long estimateSize() {
    if (value == null) {
      return getCountOnceObjectSize();
    }
    throw new UnsupportedOperationException("Cannot estimate size with literal type " + typeName);
  }

  /**
   * @return value of {@link #typeName}
   */
  public SqlTypeName getTypeName() {
    return typeName;
  }

  /**
   * @return whether value is appropriate for its type (we have rules about
   * these things)
   */
  public static boolean valueMatchesType(
      Object value,
      SqlTypeName typeName) {
    switch (typeName) {
    case BOOLEAN:
      return (value == null) || (value instanceof Boolean);
    case NULL:
      return value == null;
    case USER_SYMBOL:
      return value == null;
    case DECIMAL:
    case DOUBLE:
      return value instanceof BigDecimal;
    case DATE:
      return value instanceof DateString;
    case TIME:
      return value instanceof TimeString;
    case TIMESTAMP:
      return value instanceof TimestampString;
    case INTERVAL:
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_DAY_MICROSECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_HOUR_MICROSECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_MINUTE_MICROSECOND:
    case INTERVAL_SECOND:
    case INTERVAL_SECOND_MICROSECOND:
    case INTERVAL_MICROSECOND:
      return value instanceof SqlIntervalLiteral.IntervalValue;
    case BINARY:
      return value instanceof BitString;
    case CHAR:
      return value instanceof NlsString;
    case SYMBOL:
      return (value instanceof Enum)
          || (value instanceof SqlSampleSpec)
          || (value instanceof String);
    case MULTISET:
      return true;
    case MEDIUMINT:
    case MEDIUMINT_UNSIGNED:
    case TINYINT:
    case TINYINT_UNSIGNED:
    case SIGNED:
    case INTEGER:
      return value instanceof Integer;
    case BIGINT:
    case INTEGER_UNSIGNED:
    case UNSIGNED:
      return value instanceof Long;
    case BIGINT_UNSIGNED:
      return value instanceof BigInteger || value instanceof BigDecimal || value instanceof UInt64;
    case VARCHAR: // not allowed -- use Char
    case VARBINARY: // not allowed -- use Binary
    default:
      throw Util.unexpected(typeName);
    }
  }

  public SqlLiteral clone(SqlParserPos pos) {
    return new SqlLiteral(value, typeName, pos);
  }

  public SqlKind getKind() {
    return SqlKind.LITERAL;
  }

  /**
   * Returns the value of this literal.
   *
   * <p>Try not to use this method! There are so many different kinds of
   * values, it's better to to let SqlLiteral do whatever it is you want to
   * do.
   *
   * @see #booleanValue()
   * @see #symbolValue(Class)
   */
  public Object getValue() {
    return value;
  }

  @Deprecated
  public <T> T getValueAs(Class<T> clazz) {
    if (clazz.isInstance(value)) {
      return clazz.cast(value);
    }
    switch (typeName) {
    case CHAR:
      if (clazz == String.class && value instanceof NlsString) {
        return clazz.cast(((NlsString) value).getValue());
      }
      break;
    case BINARY:
      if (clazz == byte[].class && value instanceof BitString) {
        return clazz.cast(((BitString) value).getAsByteArray());
      }
      break;
    case DECIMAL:
      if (clazz == Long.class && value instanceof BigDecimal) {
        return clazz.cast(((BigDecimal) value).unscaledValue().longValue());
      }
      // fall through
    case BIGINT:
    case BIGINT_UNSIGNED:
    case INTEGER:
    case INTEGER_UNSIGNED:
    case SMALLINT_UNSIGNED:
    case SMALLINT:
    case MEDIUMINT:
    case MEDIUMINT_UNSIGNED:
    case TINYINT:
    case TINYINT_UNSIGNED:
    case DOUBLE:
    case REAL:
    case FLOAT:
      BigDecimal newValue = new BigDecimal(value.toString());
      if (clazz == Long.class) {
        return clazz.cast(newValue.longValue());
      } else if (clazz == Integer.class) {
        return clazz.cast(newValue.intValue());
      } else if (clazz == Short.class) {
        return clazz.cast(newValue.shortValue());
      } else if (clazz == Byte.class) {
        return clazz.cast(newValue.byteValue());
      } else if (clazz == Double.class) {
        return clazz.cast(newValue.doubleValue());
      } else if (clazz == Float.class) {
        return clazz.cast(newValue.floatValue());
      } else if (clazz == BigDecimal.class) {
        return clazz.cast(newValue);
      } else if (clazz == BigInteger.class) {
        return clazz.cast(newValue.toBigInteger());
      } else if (clazz == UInt64.class) {
        return clazz.cast(UInt64.fromBigInteger(newValue.toBigInteger()));
      }
      break;
    case DATE:
      if (clazz == Calendar.class) {
        return clazz.cast(((DateString) value).toCalendar());
      }
      break;
    case TIME:
      if (clazz == Calendar.class) {
        return clazz.cast(((TimeString) value).toCalendar());
      }
      break;
    case TIMESTAMP:
      if (clazz == Calendar.class) {
        return clazz.cast(((TimestampString) value).toCalendar());
      }
      break;
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
      final SqlIntervalLiteral.IntervalValue valMonth =
          (SqlIntervalLiteral.IntervalValue) value;
      if (clazz == Long.class) {
        return clazz.cast(valMonth.getSign()
            * SqlParserUtil.intervalToMonths(valMonth));
      } else if (clazz == BigDecimal.class) {
        return clazz.cast(BigDecimal.valueOf(getValueAs(Long.class)));
      } else if (clazz == TimeUnitRange.class) {
        return clazz.cast(valMonth.getIntervalQualifier().timeUnitRange);
      }
      break;
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
    case INTERVAL_MICROSECOND:
    case INTERVAL_DAY_MICROSECOND:
    case INTERVAL_HOUR_MICROSECOND:
    case INTERVAL_MINUTE_MICROSECOND:
    case INTERVAL_SECOND_MICROSECOND:
      final SqlIntervalLiteral.IntervalValue valTime =
          (SqlIntervalLiteral.IntervalValue) value;
      if (clazz == Long.class) {
        return clazz.cast(valTime.getSign()
            * SqlParserUtil.intervalToMillis(valTime));
      } else if (clazz == BigDecimal.class) {
        return clazz.cast(BigDecimal.valueOf(getValueAs(Long.class)));
      } else if (clazz == TimeUnitRange.class) {
        return clazz.cast(valTime.getIntervalQualifier().timeUnitRange);
      }
      break;
    }
    if (clazz == String.class) {
      return (T) String.valueOf(value);
    }
    throw new AssertionError("cannot cast " + value + " as " + clazz);
  }

  /** Returns the value as a symbol. */
  @Deprecated // to be removed before 2.0
  public <E extends Enum<E>> E symbolValue_() {
    //noinspection unchecked
    return (E) value;
  }

  /** Returns the value as a symbol. */
  public <E extends Enum<E>> E symbolValue(Class<E> class_) {
    return class_.cast(value);
  }

  /** Returns the value as a boolean. */
  public boolean booleanValue() {
    return (Boolean) value;
  }

  /**
   * Extracts the {@link SqlSampleSpec} value from a symbol literal.
   *
   * @throws ClassCastException if the value is not a symbol literal
   * @see #createSymbol(Enum, SqlParserPos)
   */
  public static SqlSampleSpec sampleValue(SqlNode node) {
    return (SqlSampleSpec) ((SqlLiteral) node).value;
  }

  /**
   * Extracts the value from a literal.
   *
   * <p>Cases:
   * <ul>
   * <li>If the node is a character literal, a chain of string
   * literals, or a CAST of a character literal, returns the value as a
   * {@link NlsString}.
   *
   * <li>If the node is a numeric literal, or a negated numeric literal,
   * returns the value as a {@link BigDecimal}.
   *
   * <li>If the node is a {@link SqlIntervalQualifier},
   * returns its {@link TimeUnitRange}.
   *
   * <li>If the node is INTERVAL_DAY_TIME_ in {@link SqlTypeFamily},
   * returns its sign multiplied by its millisecond equivalent value
   *
   * <li>If the node is INTERVAL_YEAR_MONTH_ in {@link SqlTypeFamily},
   * returns its sign multiplied by its months equivalent value
   *
   * <li>Otherwise throws {@link IllegalArgumentException}.
   * </ul>
   */
  public static Comparable value(SqlNode node)
      throws IllegalArgumentException {
    if (node instanceof SqlLiteral) {
      final SqlLiteral literal = (SqlLiteral) node;
      if (literal.getTypeName() == SqlTypeName.SYMBOL) {
        return (Enum) literal.value;
      }
      switch (literal.getTypeName().getFamily()) {
      case CHARACTER:
        return (NlsString) literal.value;
      case NUMERIC:
        return (BigDecimal) literal.value;
      case INTERVAL_YEAR_MONTH:
        final SqlIntervalLiteral.IntervalValue valMonth =
            (SqlIntervalLiteral.IntervalValue) literal.value;
        return new IntervalString(valMonth.getSign(), valMonth.getIntervalLiteral());
      case INTERVAL_DAY_TIME:
        final SqlIntervalLiteral.IntervalValue valTime =
            (SqlIntervalLiteral.IntervalValue) literal.value;
        return new IntervalString(valTime.getSign(), valTime.getIntervalLiteral());
      }
    }
    if (SqlUtil.isLiteralChain(node)) {
      assert node instanceof SqlCall;
      final SqlLiteral literal =
          SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
      assert SqlTypeUtil.inCharFamily(literal.getTypeName());
      return (NlsString) literal.value;
    }
    if (node instanceof SqlIntervalQualifier) {
      SqlIntervalQualifier qualifier = (SqlIntervalQualifier) node;
      return qualifier.timeUnitRange;
    }
    switch (node.getKind()) {
    case CAST:
      assert node instanceof SqlCall;
      return value(((SqlCall) node).operand(0));
    case MINUS_PREFIX:
      assert node instanceof SqlCall;
      Comparable o = value(((SqlCall) node).operand(0));
      if (o instanceof BigDecimal) {
        BigDecimal bigDecimal = (BigDecimal) o;
        return bigDecimal.negate();
      }
      // fall through
    default:
      throw new IllegalArgumentException("not a literal: " + node);
    }
  }

  /**
   * Extracts the string value from a string literal, a chain of string
   * literals, or a CAST of a string literal.
   *
   * @deprecated Use {@link #value(SqlNode)}
   */
  @Deprecated // to be removed before 2.0
  public static String stringValue(SqlNode node) {
    if (node instanceof SqlLiteral) {
      SqlLiteral literal = (SqlLiteral) node;
      assert SqlTypeUtil.inCharFamily(literal.getTypeName());
      return literal.value.toString();
    } else if (SqlUtil.isLiteralChain(node)) {
      final SqlLiteral literal =
          SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
      assert SqlTypeUtil.inCharFamily(literal.getTypeName());
      return literal.value.toString();
    } else if (node instanceof SqlCall
        && ((SqlCall) node).getOperator() == SqlStdOperatorTable.CAST) {
      //noinspection deprecation
      return stringValue(((SqlCall) node).operand(0));
    } else {
      throw new AssertionError("invalid string literal: " + node);
    }
  }

  /**
   * Converts a chained string literals into regular literals; returns regular
   * literals unchanged.
   */
  public static SqlLiteral unchain(SqlNode node) {
    if (node instanceof SqlLiteral) {
      return (SqlLiteral) node;
    } else if (SqlUtil.isLiteralChain(node)) {
      return SqlLiteralChainOperator.concatenateOperands((SqlCall) node);
    } else if (node instanceof SqlIntervalQualifier) {
      final SqlIntervalQualifier q = (SqlIntervalQualifier) node;
      return new SqlLiteral(
          new SqlIntervalLiteral.IntervalValue(q, 1, q.toString()),
          q.typeName(), q.pos);
    } else {
      throw new AssertionError("invalid literal: " + node);
    }
  }

  /**
   * For calc program builder - value may be different than {@link #unparse}
   * Typical values:
   *
   * <ul>
   * <li>Hello, world!</li>
   * <li>12.34</li>
   * <li>{null}</li>
   * <li>1969-04-29</li>
   * </ul>
   *
   * @return string representation of the value
   */
  public String toValue() {
    if (value == null) {
      return null;
    }
    switch (typeName) {
    case CHAR:

      // We want 'It''s superman!', not _ISO-8859-1'It''s superman!'
      return ((NlsString) value).getValue();
    default:
      return value.toString();
    }
  }

  public void validate(SqlValidator validator, SqlValidatorScope scope) {
    validator.validateLiteral(this);
  }

  public <R> R accept(SqlVisitor<R> visitor) {
    return visitor.visit(this);
  }

  public boolean equalsDeep(SqlNode node, Litmus litmus, EqualsContext context) {
    if (!(node instanceof SqlLiteral)) {
      return litmus.fail("{} != {}", this, node);
    }
    SqlLiteral that = (SqlLiteral) node;
    if (!this.equals(that)) {
      return litmus.fail("{} != {}", this, node);
    }
    return litmus.succeed();
  }

  public SqlMonotonicity getMonotonicity(SqlValidatorScope scope) {
    return SqlMonotonicity.CONSTANT;
  }

  /**
   * Creates a NULL literal.
   *
   * <p>There's no singleton constant for a NULL literal. Instead, nulls must
   * be instantiated via createNull(), because different instances have
   * different context-dependent types.
   */
  public static SqlLiteral createNull(SqlParserPos pos) {
    return new SqlLiteral(null, SqlTypeName.NULL, pos);
  }

  /**
   * Creates a boolean literal.
   */
  public static SqlLiteral createBoolean(
      boolean b,
      SqlParserPos pos) {
    return b ? new SqlLiteral(Boolean.TRUE, SqlTypeName.BOOLEAN, pos)
        : new SqlLiteral(Boolean.FALSE, SqlTypeName.BOOLEAN, pos);
  }

  public static SqlLiteral createUnknown(SqlParserPos pos) {
    return new SqlLiteral(null, SqlTypeName.BOOLEAN, pos);
  }

  /**
   * Creates a literal which represents a parser symbol, for example the
   * <code>TRAILING</code> keyword in the call <code>Trim(TRAILING 'x' FROM
   * 'Hello world!')</code>.
   *
   * @see #symbolValue(Class)
   */
  public static SqlLiteral createSymbol(Enum<?> o, SqlParserPos pos) {
    return new SqlLiteral(o, SqlTypeName.SYMBOL, pos);
  }

  /**
   * Create symbol by string
   * @param o
   * @param pos
   * @return
   */
  public static SqlLiteral createSymbol(String o, SqlParserPos pos) {
    return new SqlLiteral(o, SqlTypeName.SYMBOL, pos);
  }

  /**
   * Creates a literal which represents a sample specification.
   */
  public static SqlLiteral createSample(
      SqlSampleSpec sampleSpec,
      SqlParserPos pos) {
    return new SqlLiteral(sampleSpec, SqlTypeName.SYMBOL, pos);
  }

  public boolean equals(Object obj) {
    if (!(obj instanceof SqlLiteral)) {
      return false;
    }
    SqlLiteral that = (SqlLiteral) obj;
    return Objects.equals(value, that.value);
  }

  public int hashCode() {
    return (value == null) ? 0 : value.hashCode();
  }

  /**
   * Returns the integer value of this literal.
   *
   * @param exact Whether the value has to be exact. If true, and the literal
   *              is a fraction (e.g. 3.14), throws. If false, discards the
   *              fractional part of the value.
   * @return Integer value of this literal
   */
    public int intValue(boolean exact) {
        switch (typeName) {
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case INTEGER:
            case SIGNED:
                return (int) value;
            case INTEGER_UNSIGNED:
            case UNSIGNED:
            case BIGINT:
                return ((Long) value).intValue();
            case BIGINT_UNSIGNED:
                return ((BigInteger) value).intValue();
            case DECIMAL:
            case DOUBLE:
                BigDecimal bd = (BigDecimal) value;
                if (exact) {
                    try {
                        return bd.intValueExact();
                    } catch (ArithmeticException e) {
                        throw SqlUtil.newContextException(getParserPosition(),
                            RESOURCE.numberLiteralOutOfRange(bd.toString()));
                    }
                } else {
                    return bd.intValue();
                }
            default:
                throw Util.unexpected(typeName);
        }
    }

  /**
   * Returns the long value of this literal.
   *
   * @param exact Whether the value has to be exact. If true, and the literal
   *              is a fraction (e.g. 3.14), throws. If false, discards the
   *              fractional part of the value.
   * @return Long value of this literal
   */
    public long longValue(boolean exact) {
        switch (typeName) {
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case INTEGER:
            case SIGNED:
                return ((Integer) value).longValue();
            case INTEGER_UNSIGNED:
            case UNSIGNED:
            case BIGINT:
                return ((Long) value).longValue();
            case BIGINT_UNSIGNED:
                return ((BigInteger) value).longValue();
            case DECIMAL:
            case DOUBLE:
                BigDecimal bd = (BigDecimal) value;
                if (exact) {
                    try {
                        return bd.longValueExact();
                    } catch (ArithmeticException e) {
                        throw SqlUtil.newContextException(getParserPosition(),
                            RESOURCE.numberLiteralOutOfRange(bd.toString()));
                    }
                } else {
                    return bd.longValue();
                }
            default:
                throw Util.unexpected(typeName);
        }
    }

  /**
   * Returns sign of value.
   *
   * @return -1, 0 or 1
   */
  @Deprecated // to be removed before 2.0
  public int signum() {
    return bigDecimalValue().compareTo(
        BigDecimal.ZERO);
  }

    /**
     * Returns a numeric literal's value as a {@link BigDecimal}.
     */
    public BigDecimal bigDecimalValue() {
        switch (typeName) {
            case MEDIUMINT:
            case MEDIUMINT_UNSIGNED:
            case SMALLINT:
            case SMALLINT_UNSIGNED:
            case TINYINT:
            case TINYINT_UNSIGNED:
            case INTEGER:
            case SIGNED:
                return new BigDecimal((Integer) value);
            case INTEGER_UNSIGNED:
            case UNSIGNED:
            case BIGINT:
                return BigDecimal.valueOf((Long) value);
            case BIGINT_UNSIGNED:
                return new BigDecimal((BigInteger) value);
            case DECIMAL:
            case DOUBLE:
                return (BigDecimal) value;
            default:
                throw Util.unexpected(typeName);
        }
    }

  @Deprecated // to be removed before 2.0
  public String getStringValue() {
    return ((NlsString) value).getValue();
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    switch (typeName) {
    case BOOLEAN:
      if (writer.getDialect().getDatabaseProduct() == SqlDialect.DatabaseProduct.ORACLE) {
        writer.keyword(
            value == null ? "UNKNOWN" : (Boolean) value ? "1" : "0");
      } else {
        writer.keyword(
            value == null ? "UNKNOWN" : (Boolean) value ? "TRUE" : "FALSE");
      }
      break;
    case NULL:
      writer.keyword("NULL");
      break;
    case CHAR:
    case DECIMAL:
    case DOUBLE:
    case BINARY:
      // should be handled in subtype
      throw Util.unexpected(typeName);

    case SYMBOL:
      if (value instanceof Enum) {
        Enum enumVal = (Enum) value;
        writer.keyword(enumVal.toString());
      } else {
        writer.keyword(String.valueOf(value));
      }
      break;
    default:
      writer.literal(value.toString());
    }
  }

  public RelDataType createSqlType(RelDataTypeFactory typeFactory) {
    BitString bitString;
    switch (typeName) {
    case NULL:
    case BOOLEAN:
      RelDataType ret = typeFactory.createSqlType(typeName);
      ret = typeFactory.createTypeWithNullability(ret, null == value);
      return ret;
    case BINARY:
      bitString = (BitString) value;
      int bitCount = bitString.getBitCount();
      return typeFactory.createSqlType(SqlTypeName.BINARY, bitCount / 8);
    case CHAR:
      NlsString string = (NlsString) value;
      return SqlUtil.createNlsStringType(typeFactory, string);
    case INTERVAL_YEAR:
    case INTERVAL_YEAR_MONTH:
    case INTERVAL_MONTH:
    case INTERVAL_DAY:
    case INTERVAL_DAY_HOUR:
    case INTERVAL_DAY_MINUTE:
    case INTERVAL_DAY_SECOND:
    case INTERVAL_HOUR:
    case INTERVAL_HOUR_MINUTE:
    case INTERVAL_HOUR_SECOND:
    case INTERVAL_MINUTE:
    case INTERVAL_MINUTE_SECOND:
    case INTERVAL_SECOND:
    case INTERVAL_DAY_MICROSECOND:
    case INTERVAL_HOUR_MICROSECOND:
      SqlIntervalLiteral.IntervalValue intervalValue =
          (SqlIntervalLiteral.IntervalValue) value;
      return typeFactory.createSqlIntervalType(
          intervalValue.getIntervalQualifier());

    case SYMBOL:
      return typeFactory.createSqlType(SqlTypeName.SYMBOL);
    case USER_SYMBOL:
      return typeFactory.createSqlType(SqlTypeName.USER_SYMBOL);

    case INTEGER: // handled in derived class
    case TIME: // handled in derived class
    case VARCHAR: // should never happen
    case VARBINARY: // should never happen

    default:
      throw Util.needToImplement(toString() + ", operand=" + value);
    }
  }

  @Deprecated // to be removed before 2.0
  public static SqlDateLiteral createDate(
      Calendar calendar,
      SqlParserPos pos) {
    return createDate(DateString.fromCalendarFields(calendar), pos);
  }

  public static SqlDateLiteral createDate(
      DateString date,
      SqlParserPos pos) {
    return new SqlDateLiteral(date, pos);
  }

  @Deprecated // to be removed before 2.0
  public static SqlTimestampLiteral createTimestamp(
      Calendar calendar,
      int precision,
      SqlParserPos pos) {
    return createTimestamp(TimestampString.fromCalendarFields(calendar), precision, pos);
  }

  public static SqlTimestampLiteral createTimestamp(
      TimestampString ts,
      int precision,
      SqlParserPos pos) {
    return new SqlTimestampLiteral(ts, precision, false, pos);
  }

  @Deprecated // to be removed before 2.0
  public static SqlTimeLiteral createTime(
      Calendar calendar,
      int precision,
      SqlParserPos pos) {
    return createTime(TimeString.fromCalendarFields(calendar), precision, pos);
  }

  public static SqlTimeLiteral createTime(
      TimeString t,
      int precision,
      SqlParserPos pos) {
    return new SqlTimeLiteral(t, precision, false, pos);
  }

  /**
   * Creates an interval literal.
   *
   * @param intervalStr       input string of '1:23:04'
   * @param intervalQualifier describes the interval type and precision
   * @param pos               Parser position
   */
  public static SqlIntervalLiteral createInterval(
      int sign,
      String intervalStr,
      SqlIntervalQualifier intervalQualifier,
      SqlParserPos pos) {
    return new SqlIntervalLiteral(sign, intervalStr, intervalQualifier,
        intervalQualifier.typeName(), pos);
  }

  public static SqlNumericLiteral createNegative(
      SqlNumericLiteral num,
      SqlParserPos pos) {
    return new SqlNumericLiteral(
        ((BigDecimal) num.getValue()).negate(),
        num.getPrec(),
        num.getScale(),
        num.isExact(),
        pos);
  }

  public static SqlNumericLiteral createLiteralForIntTypes(Long value, SqlParserPos pos, SqlTypeName typeName) {
    return new SqlNumericLiteral(value, Long.toString(value).length(), 0, true, pos, typeName);
  }

  public static SqlNumericLiteral createLiteralForIntTypes(String s, SqlParserPos pos, SqlTypeName typeName) {
    Comparable value = null;
    if (SqlTypeFamily.EXACT_INTEGER.contains(typeName)) {
      value = Integer.valueOf(s);
    } else if (SqlTypeFamily.EXACT_BIGINT.contains(typeName)) {
      value = Long.valueOf(s);
    }

    return createExactNumeric(value, s, pos, typeName);
  }

  public static SqlNumericLiteral createExactNumeric(BigDecimal value, SqlParserPos pos) {
    int prec = value.precision();
    int scale = value.scale();
    return new SqlNumericLiteral(
        value,
        prec,
        scale,
        true,
        pos);
  }

  public static SqlNumericLiteral createExactNumeric(
      String s,
      SqlParserPos pos) {
    BigDecimal value;
    int prec;
    int scale;

    int i = s.indexOf('.');
    if ((i >= 0) && ((s.length() - 1) != i)) {
      value = SqlParserUtil.parseDecimal(s);
      scale = s.length() - i - 1;
      assert scale == value.scale() : s;
      prec = s.length() - 1;
    } else if ((i >= 0) && ((s.length() - 1) == i)) {
      value = SqlParserUtil.parseInteger(s.substring(0, i));
      scale = 0;
      prec = s.length() - 1;
    } else {
      value = SqlParserUtil.parseInteger(s);
      scale = 0;
      prec = s.length();
    }
    return new SqlNumericLiteral(
        value,
        prec,
        scale,
        true,
        pos);
  }

    public static SqlNumericLiteral createExactNumeric(Comparable value, String s, SqlParserPos pos,
                                                       SqlTypeName typeName) {
        int prec;
        int scale;

        int i = s.indexOf('.');
        if ((i >= 0) && ((s.length() - 1) != i)) {
            BigDecimal decimalValue;
            decimalValue = SqlParserUtil.parseDecimal(s);
            scale = s.length() - i - 1;
            assert scale == decimalValue.scale() : s;
            prec = s.length() - 1;
            value = decimalValue;
        } else if ((i >= 0) && ((s.length() - 1) == i)) {
            if (value == null) {
                value = SqlParserUtil.parseInteger(s.substring(0, i));
            }
            scale = 0;
            prec = s.length() - 1;
        } else {
            if (value == null) {
                value = SqlParserUtil.parseInteger(s);
            }
            scale = 0;
            prec = s.length();
        }
        return new SqlNumericLiteral(value, prec, scale, true, pos, typeName);
    }

  public static SqlNumericLiteral createApproxNumeric(
      String s,
      SqlParserPos pos) {
    BigDecimal value = SqlParserUtil.parseDecimal(s);
    return new SqlNumericLiteral(value, null, null, false, pos);
  }

  /**
   * Creates a literal like X'ABAB'. Although it matters when we derive a type
   * for this beastie, we don't care at this point whether the number of
   * hexits is odd or even.
   */
  public static SqlBinaryStringLiteral createBinaryString(
      String s,
      SqlParserPos pos) {
    BitString bits;
    try {
      bits = BitString.createFromHexString(s);
    } catch (NumberFormatException e) {
      throw SqlUtil.newContextException(pos,
          RESOURCE.binaryLiteralInvalid());
    }
    return new SqlBinaryStringLiteral(bits, pos);
  }

  /**
   * Create binary string.
   *
   * @param s
   * @param pos
   * @param fromHex
     * @return
     */
  public static SqlBinaryStringLiteral createBinaryString(
      String s,
      SqlParserPos pos,
      boolean fromHex) {
    if (fromHex) {
      return createBinaryString(s, pos);
    } else {
      BitString bits;
      try {
        bits = BitString.createFromBitString(s);
      } catch (NumberFormatException e) {
        throw SqlUtil.newContextException(pos,
            RESOURCE.binaryLiteralInvalid());
      }
      return new SqlBinaryStringLiteral(bits,!fromHex , pos);
    }
  }

  /**
   * Creates a literal like X'ABAB' from an array of bytes.
   *
   * @param bytes Contents of binary literal
   * @param pos   Parser position
   * @return Binary string literal
   */
  public static SqlBinaryStringLiteral createBinaryString(
      byte[] bytes,
      SqlParserPos pos) {
    BitString bits;
    try {
      bits = BitString.createFromBytes(bytes);
    } catch (NumberFormatException e) {
      throw SqlUtil.newContextException(pos, RESOURCE.binaryLiteralInvalid());
    }
    return new SqlBinaryStringLiteral(bits, pos);
  }

  /**
   * Creates a literal like X'ABAB' from an array of bytes.
   *
   * @param bitString
   * @param pos   Parser position
   * @return Binary string literal
   */
  public static SqlBinaryStringLiteral createBinaryString(
          BitString bitString,
          SqlParserPos pos) {
    return new SqlBinaryStringLiteral(bitString, pos);
  }

  /**
   * Creates a string literal in the system character set.
   *
   * @param s   a string (without the sql single quotes)
   * @param pos Parser position
   */
  public static SqlCharStringLiteral createCharString(
      String s,
      SqlParserPos pos) {
    // UnsupportedCharsetException not possible
    return createCharString(s, null, pos);
  }

  /**
   * Creates a string literal, with optional character-set.
   *
   * @param s       a string (without the sql single quotes)
   * @param charSet character set name, null means take system default
   * @param pos     Parser position
   * @return A string literal
   * @throws UnsupportedCharsetException if charSet is not null but there is
   *                                     no character set with that name in this
   *                                     environment
   */
  public static SqlCharStringLiteral createCharString(
      String s,
      String charSet,
      SqlParserPos pos) {
    NlsString slit = new NlsString(s, charSet, null);
    return new SqlCharStringLiteral(slit, pos);
  }

  public static SqlCharStringLiteral createCharString(NlsString nlsString, SqlParserPos pos) {
    return new SqlCharStringLiteral(nlsString, pos);
  }

  /**
   * Transforms this literal (which must be of type character) into a new one
   * in which 4-digit Unicode escape sequences have been replaced with the
   * corresponding Unicode characters.
   *
   * @param unicodeEscapeChar escape character (e.g. backslash) for Unicode
   *                          numeric sequences; 0 implies no transformation
   * @return transformed literal
   */
  public SqlLiteral unescapeUnicode(char unicodeEscapeChar) {
    if (unicodeEscapeChar == 0) {
      return this;
    }
    assert SqlTypeUtil.inCharFamily(getTypeName());
    NlsString ns = (NlsString) value;
    String s = ns.getValue();
    StringBuilder sb = new StringBuilder();
    int n = s.length();
    for (int i = 0; i < n; ++i) {
      char c = s.charAt(i);
      if (c == unicodeEscapeChar) {
        if (n > (i + 1)) {
          if (s.charAt(i + 1) == unicodeEscapeChar) {
            sb.append(unicodeEscapeChar);
            ++i;
            continue;
          }
        }
        if ((i + 5) > n) {
          throw SqlUtil.newContextException(getParserPosition(),
              RESOURCE.unicodeEscapeMalformed(i));
        }
        final String u = s.substring(i + 1, i + 5);
        final int v;
        try {
          v = Integer.parseInt(u, 16);
        } catch (NumberFormatException ex) {
          throw SqlUtil.newContextException(getParserPosition(),
              RESOURCE.unicodeEscapeMalformed(i));
        }
        sb.append((char) (v & 0xFFFF));

        // skip hexits
        i += 4;
      } else {
        sb.append(c);
      }
    }
    ns = new NlsString(
        sb.toString(),
        ns.getCharsetName(),
        ns.getCollation());
    return new SqlCharStringLiteral(ns, getParserPosition());
  }

  //~ Inner Interfaces -------------------------------------------------------

  /**
   * A value must implement this interface if it is to be embedded as a
   * SqlLiteral of type SYMBOL. If the class is an {@link Enum} it trivially
   * implements this interface.
   *
   * <p>The {@link #toString()} method should return how the symbol should be
   * unparsed, which is sometimes not the same as the enumerated value's name
   * (e.g. "UNBOUNDED PRECEDING" versus "UnboundedPreceeding").
   */
  @Deprecated // to be removed before 2.0
  public interface SqlSymbol {
    String name();

    int ordinal();
  }
}

// End SqlLiteral.java
