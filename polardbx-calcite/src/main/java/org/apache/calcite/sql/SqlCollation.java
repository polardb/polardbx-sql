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

import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.charset.CollationName;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.parser.SqlParserUtil;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.Glossary;
import org.apache.calcite.util.SaffronProperties;
import org.apache.calcite.util.SerializableCharset;
import org.apache.calcite.util.Util;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

import static org.apache.calcite.util.Static.RESOURCE;

/**
 * A <code>SqlCollation</code> is an object representing a <code>Collate</code>
 * statement. It is immutable.
 */
public class SqlCollation implements Serializable {
  public static final SqlCollation COERCIBLE =
      new SqlCollation(Coercibility.COERCIBLE);
  public static final SqlCollation IMPLICIT =
      new SqlCollation(
          CharsetName.defaultCharset().toJavaCharset(),
          CollationName.defaultCollation().name(),
          Coercibility.IMPLICIT
      );

  //~ Enums ------------------------------------------------------------------

  /**
   * <blockquote>A &lt;character value expression&gt; consisting of a column
   * reference has the coercibility characteristic Implicit, with collating
   * sequence as defined when the column was created. A &lt;character value
   * expression&gt; consisting of a value other than a column (e.g., a host
   * variable or a literal) has the coercibility characteristic Coercible,
   * with the default collation for its character repertoire. A &lt;character
   * value expression&gt; simply containing a &lt;collate clause&gt; has the
   * coercibility characteristic Explicit, with the collating sequence
   * specified in the &lt;collate clause&gt;.</blockquote>
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3
   */
  public enum Coercibility {
    /** Strongest coercibility. */
    EXPLICIT(4),
    IMPLICIT(3),
    COERCIBLE(2),
    /** Weakest coercibility. */
    NONE(1);

    final int priority;
    Coercibility(int priority) {
      this.priority = priority;
    }

    int getPriority() {
      return this.priority;
    }
  }

  //~ Instance fields --------------------------------------------------------

  protected String collationName;
  protected final SerializableCharset wrappedCharset;
  protected Locale locale;
  protected String strength;
  private final Coercibility coercibility;
  private boolean sensitive = false;

  //~ Constructors -----------------------------------------------------------

  /**
   * Creates a Collation by its name and its coercibility
   * Deprecated cause it's not compatible with MySQL collation name.
   *
   * @param collation    Collation specification
   * @param coercibility Coercibility
   */
  @Deprecated
  public SqlCollation(
      String collation,
      Coercibility coercibility) {
    this.coercibility = coercibility;
    SqlParserUtil.ParsedCollation parseValues =
        SqlParserUtil.parseCollation(collation);
    Charset charset = parseValues.getCharset();
    this.wrappedCharset = SerializableCharset.forCharset(charset);
    locale = parseValues.getLocale();
    strength = parseValues.getStrength();
    String c =
        charset.name().toUpperCase(Locale.ROOT) + "$" + locale.toString();
    if ((strength != null) && (strength.length() > 0)) {
      c += "$" + strength;
    }
    collationName = c;
  }

  public SqlCollation(
      final Charset charset,
      final String collation,
      final Coercibility coercibility) {
    // check invalidity of charset & collation.
    Objects.requireNonNull(charset);
    Objects.requireNonNull(coercibility);
    String resultCollation;
    Charset resultCharset;
    if (collation != null) {
      // the charset should be consistent with collation.
      resultCollation = Optional.ofNullable(collation)
          .map(CollationName::of)
          .map(Enum::name)
          .orElseGet(
              () -> CollationName.defaultCollation().name()
          );

      resultCharset = Optional.of(charset)
          .map(CharsetName::of)
          .filter(c -> c.isSupported(resultCollation))
          .map(CharsetName::toJavaCharset)
          .orElseGet(
              () -> CollationName.getCharsetOf(resultCollation).toJavaCharset()
          );
    } else {
      resultCollation = Optional.of(charset)
          .map(CharsetName::of)
          .map(CharsetName::getDefaultCollationName)
          .map(Enum::name)
          .get();

      resultCharset = charset;
    }

    this.coercibility = coercibility;
    this.wrappedCharset = SerializableCharset.forCharset(resultCharset);
    this.collationName = resultCollation;
  }

  /**
   * Creates a SqlCollation with the default collation name and the given
   * coercibility.
   *
   * @param coercibility Coercibility
   */
  public SqlCollation(Coercibility coercibility) {
    this(
        SaffronProperties.INSTANCE.defaultCollation().get(),
        coercibility);
  }

  //~ Methods ----------------------------------------------------------------

  public boolean equals(Object o) {
    return this == o
        || o instanceof SqlCollation
        && collationName.equals(((SqlCollation) o).collationName);
  }

  @Override public int hashCode() {
    return collationName.hashCode();
  }

  /**
   * Returns the collating sequence (the collation name) and the coercibility
   * for the resulting value of a dyadic operator.
   *
   * @param col1 first operand for the dyadic operation
   * @param col2 second operand for the dyadic operation
   * @return the resulting collation sequence. The "no collating sequence"
   * result is returned as null.
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
   */
  public static SqlCollation getCoercibilityDyadicOperator(
      SqlCollation col1,
      SqlCollation col2) {
    return getCoercibilityDyadic(col1, col2);
  }

  /**
   * Returns the collating sequence (the collation name) and the coercibility
   * for the resulting value of a dyadic operator.
   *
   * @param col1 first operand for the dyadic operation
   * @param col2 second operand for the dyadic operation
   * @return the resulting collation sequence
   *
   * @throws org.apache.calcite.runtime.CalciteException from
   *   {@link org.apache.calcite.runtime.CalciteResource#invalidCompare} or
   *   {@link org.apache.calcite.runtime.CalciteResource#differentCollations}
   *   if no collating sequence can be deduced
   *
   * @see Glossary#SQL99 SQL:1999 Part 2 Section 4.2.3 Table 2
   */
  public static SqlCollation getCoercibilityDyadicOperatorThrows(
      SqlCollation col1,
      SqlCollation col2) {
    SqlCollation ret = getCoercibilityDyadic(col1, col2);
    if (null == ret) {
      throw RESOURCE.invalidCompare(
          col1.collationName,
          "" + col1.coercibility,
          col2.collationName,
          "" + col2.coercibility).ex();
    }
    return ret;
  }

  protected static SqlCollation getCoercibilityDyadic(
      SqlCollation col1,
      SqlCollation col2) {
    assert null != col1;
    assert null != col2;
    final Coercibility coercibility1 = col1.getCoercibility();
    final Coercibility coercibility2 = col2.getCoercibility();
    switch (coercibility1) {
    case COERCIBLE:
      switch (coercibility2) {
      case COERCIBLE:
        return new SqlCollation(
            col2.collationName,
            Coercibility.COERCIBLE);
      case IMPLICIT:
        return new SqlCollation(
            col2.collationName,
            Coercibility.IMPLICIT);
      case NONE:
        return null;
      case EXPLICIT:
        return new SqlCollation(
            col2.collationName,
            Coercibility.EXPLICIT);
      default:
        throw Util.unexpected(coercibility2);
      }
    case IMPLICIT:
      switch (coercibility2) {
      case COERCIBLE:
        return new SqlCollation(
            col1.collationName,
            Coercibility.IMPLICIT);
      case IMPLICIT:
        if (col1.collationName.equals(col2.collationName)) {
          return new SqlCollation(
              col2.collationName,
              Coercibility.IMPLICIT);
        }
        return null;
      case NONE:
        return null;
      case EXPLICIT:
        return new SqlCollation(
            col2.collationName,
            Coercibility.EXPLICIT);
      default:
        throw Util.unexpected(coercibility2);
      }
    case NONE:
      switch (coercibility2) {
      case COERCIBLE:
      case IMPLICIT:
      case NONE:
        return null;
      case EXPLICIT:
        return new SqlCollation(
            col2.collationName,
            Coercibility.EXPLICIT);
      default:
        throw Util.unexpected(coercibility2);
      }
    case EXPLICIT:
      switch (coercibility2) {
      case COERCIBLE:
      case IMPLICIT:
      case NONE:
        return new SqlCollation(
            col1.collationName,
            Coercibility.EXPLICIT);
      case EXPLICIT:
        if (col1.collationName.equals(col2.collationName)) {
          return new SqlCollation(
              col2.collationName,
              Coercibility.EXPLICIT);
        }
        throw RESOURCE.differentCollations(
            col1.collationName,
            col2.collationName).ex();
      default:
        throw Util.unexpected(coercibility2);
      }
    default:
      throw Util.unexpected(coercibility1);
    }
  }

  /**
   * Handle the mix of collations
   * According to: https://dev.mysql.com/doc/refman/5.7/en/charset-collation-coercibility.html
   *
   * 1. Use the collation with the lowest coercibility value.
   * If both sides have the same coercibility, then:
   *      a. If both sides are Unicode, or both sides are not Unicode, it is an error.
   *      b. If one of the sides has a Unicode character set, and another side has a non-Unicode character set,
   *          the side with Unicode character set wins, and automatic character set conversion is applied to the
   *          non-Unicode side. For example, the following statement does not return an error:
   *                  SELECT CONCAT(utf8_column, latin1_column) FROM t1;
   *          It returns a result that has a character set of utf8 and the same collation as utf8_column.
   *          Values of latin1_column are automatically converted to utf8 before concatenating.
   * 2. For an operation with operands from the same character set but that mix a _bin collation and
   *      a _ci or _cs collation, the _bin collation is used. This is similar to how operations that mix
   *      nonbinary and binary strings evaluate the operands as binary strings, applied to collations rather
   *      than data types.
   */
  public static Pair<Charset, SqlCollation> extractMixOfCollation(SqlOperatorBinding opBinding) {
    ImmutablePair<CollationName, Coercibility> result = IntStream.range(0, opBinding.getOperandCount())
        .mapToObj(opBinding::getOperandType)
        .filter(SqlTypeUtil::isCharacter)
        .map(t -> {
          CharsetName charset = Optional.ofNullable(t)
              .map(RelDataType::getCharset)
              .map(CharsetName::of)
              .orElse(CharsetName.defaultCharset());
          CollationName collation = Optional.ofNullable(t)
              .map(RelDataType::getCollation)
              .map(SqlCollation::getCollationName)
              .map(CollationName::of)
              .orElseGet(
                  () -> Optional.of(charset).map(CharsetName::getDefaultCollationName).get()
              );
          Coercibility coercibility = Optional.ofNullable(t)
              .map(RelDataType::getCollation)
              .map(SqlCollation::getCoercibility)
              .orElse(Coercibility.COERCIBLE);

          return ImmutablePair.of(collation, coercibility);
        })
        .reduce(
            (left, right) -> {
              CollationName collationName1 = left.getKey();
              CollationName collationName2 = right.getKey();
              Coercibility coercibility1 = left.getValue();
              Coercibility coercibility2 = right.getValue();

              if (coercibility1.getPriority() > coercibility2.getPriority()) {
                return left;
              } else if (coercibility1.getPriority() < coercibility2.getPriority()) {
                return right;
              } else {
                CollationName mix = CollationName.getMixOfCollation0(collationName1, collationName2);
                if (mix == null) {
                  mix = CollationName.defaultCollation();
                }
                return ImmutablePair.of(mix, Coercibility.IMPLICIT);
              }
            }
        ).orElseGet(
          () -> ImmutablePair.of(CollationName.defaultCollation(), Coercibility.COERCIBLE)
    );
    CharsetName charsetName = Optional.ofNullable(result)
        .map(ImmutablePair::getLeft)
        .map(CollationName::getCharsetOf)
        .orElse(CharsetName.defaultCharset());
    SqlCollation sqlCollation = new SqlCollation(charsetName.toJavaCharset(), result.getKey().name(), result.getValue());

    return ImmutablePair.of(charsetName.toJavaCharset(), sqlCollation);
  }

  public static CollationName getMixOfCollation(List<ImmutablePair<CollationName, SqlCollation.Coercibility>> collationPairs) {
    return collationPairs.stream()
        .reduce(
        (left, right) -> {
          CollationName collationName1 = left.getKey();
          CollationName collationName2 = right.getKey();
          Coercibility coercibility1 = left.getValue();
          Coercibility coercibility2 = right.getValue();

          if (coercibility1.getPriority() > coercibility2.getPriority()) {
            return left;
          } else if (coercibility1.getPriority() < coercibility2.getPriority()) {
            return right;
          } else {
            CollationName mix = CollationName.getMixOfCollation0(collationName1, collationName2);
            if (mix == null) {
              mix = CollationName.defaultCollation();
            }
            return ImmutablePair.of(mix, Coercibility.IMPLICIT);
          }
        }
    )
        .map(ImmutablePair::getLeft)
        .orElseGet(
            () -> CollationName.defaultCollation()
        );
  }

  /**
   * Get MySQL style error message for mix of collations.
   * The coercibility is always coercible because all collations are infered.
   */
  public static String getErrorMessage(CollationName collationName1, CollationName collationName2, Coercibility coercibility1, Coercibility coercibility2) {
    String collationMessage = "(" + collationName1.name().toLowerCase() + ", " + coercibility1.name() + ") and ("
        + collationName2.name().toLowerCase() + ", " + coercibility2.name() + ")";
    return "Illegal mix of collations " + collationMessage;
  }

  public String toString() {
    return "COLLATE " + collationName;
  }

  public void unparse(
      SqlWriter writer,
      int leftPrec,
      int rightPrec) {
    writer.keyword("COLLATE");
    writer.identifier(collationName);
  }

  public Charset getCharset() {
    return wrappedCharset.getCharset();
  }

  public final String getCollationName() {
    return collationName;
  }

  public final SqlCollation.Coercibility getCoercibility() {
    return coercibility;
  }

  public void setSensitive(boolean b){
    sensitive = b;
  }

  public boolean isSensitive(){
    return sensitive;
  }
}

// End SqlCollation.java
