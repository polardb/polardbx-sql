/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.orc.impl;

import org.apache.orc.TypeDescription;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ParserUtils {

  static TypeDescription.Category parseCategory(ParserUtils.StringPosition source) {
    StringBuilder word = new StringBuilder();
    boolean hadSpace = true;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (Character.isLetter(ch)) {
        word.append(Character.toLowerCase(ch));
        hadSpace = false;
      } else if (ch == ' ') {
        if (!hadSpace) {
          hadSpace = true;
          word.append(ch);
        }
      } else {
        break;
      }
      source.position += 1;
    }
    String catString = word.toString();
    // if there were trailing spaces, remove them.
    if (hadSpace) {
      catString = catString.trim();
    }
    if (!catString.isEmpty()) {
      for (TypeDescription.Category cat : TypeDescription.Category.values()) {
        if (cat.getName().equals(catString)) {
          return cat;
        }
      }
    }
    throw new IllegalArgumentException("Can't parse category at " + source);
  }

  static int parseInt(ParserUtils.StringPosition source) {
    int start = source.position;
    int result = 0;
    while (source.position < source.length) {
      char ch = source.value.charAt(source.position);
      if (!Character.isDigit(ch)) {
        break;
      }
      result = result * 10 + (ch - '0');
      source.position += 1;
    }
    if (source.position == start) {
      throw new IllegalArgumentException("Missing integer at " + source);
    }
    return result;
  }

  static String parseName(ParserUtils.StringPosition source) {
    if (source.position == source.length) {
      throw new IllegalArgumentException("Missing name at " + source);
    }
    final int start = source.position;
    if (source.value.charAt(source.position) == '`') {
      source.position += 1;
      StringBuilder buffer = new StringBuilder();
      boolean closed = false;
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        source.position += 1;
        if (ch == '`') {
          if (source.position < source.length &&
                  source.value.charAt(source.position) == '`') {
            source.position += 1;
            buffer.append('`');
          } else {
            closed = true;
            break;
          }
        } else {
          buffer.append(ch);
        }
      }
      if (!closed) {
        source.position = start;
        throw new IllegalArgumentException("Unmatched quote at " + source);
      } else if (buffer.length() == 0) {
        throw new IllegalArgumentException("Empty quoted field name at " + source);
      }
      return buffer.toString();
    } else {
      while (source.position < source.length) {
        char ch = source.value.charAt(source.position);
        if (!Character.isLetterOrDigit(ch) && ch != '_') {
          break;
        }
        source.position += 1;
      }
      if (source.position == start) {
        throw new IllegalArgumentException("Missing name at " + source);
      }
      return source.value.substring(start, source.position);
    }
  }

  static void requireChar(ParserUtils.StringPosition source, char required) {
    if (source.position >= source.length ||
            source.value.charAt(source.position) != required) {
      throw new IllegalArgumentException("Missing required char '" +
              required + "' at " + source);
    }
    source.position += 1;
  }

  private static boolean consumeChar(ParserUtils.StringPosition source,
                                     char ch) {
    boolean result = source.position < source.length &&
            source.value.charAt(source.position) == ch;
    if (result) {
      source.position += 1;
    }
    return result;
  }

  private static void parseUnion(TypeDescription type,
                                 ParserUtils.StringPosition source) {
    requireChar(source, '<');
    do {
      type.addUnionChild(parseType(source));
    } while (consumeChar(source, ','));
    requireChar(source, '>');
  }

  private static void parseStruct(TypeDescription type,
                                  ParserUtils.StringPosition source) {
    requireChar(source, '<');
    boolean needComma = false;
    while (!consumeChar(source, '>')) {
      if (needComma) {
        requireChar(source, ',');
      } else {
        needComma = true;
      }
      String fieldName = parseName(source);
      requireChar(source, ':');
      type.addField(fieldName, parseType(source));
    }
  }

  public static TypeDescription parseType(ParserUtils.StringPosition source) {
    TypeDescription result = new TypeDescription(parseCategory(source));
    switch (result.getCategory()) {
      case BINARY:
      case BOOLEAN:
      case BYTE:
      case DATE:
      case DOUBLE:
      case FLOAT:
      case INT:
      case LONG:
      case SHORT:
      case STRING:
      case TIMESTAMP:
      case TIMESTAMP_INSTANT:
        break;
      case CHAR:
      case VARCHAR:
        requireChar(source, '(');
        result.withMaxLength(parseInt(source));
        requireChar(source, ')');
        break;
      case DECIMAL: {
        requireChar(source, '(');
        int precision = parseInt(source);
        requireChar(source, ',');
        result.withScale(parseInt(source));
        result.withPrecision(precision);
        requireChar(source, ')');
        break;
      }
      case LIST: {
        requireChar(source, '<');
        TypeDescription child = parseType(source);
        result.addChild(child);
        requireChar(source, '>');
        break;
      }
      case MAP: {
        requireChar(source, '<');
        TypeDescription keyType = parseType(source);
        result.addChild(keyType);
        requireChar(source, ',');
        TypeDescription valueType = parseType(source);
        result.addChild(valueType);
        requireChar(source, '>');
        break;
      }
      case UNION:
        parseUnion(result, source);
        break;
      case STRUCT:
        parseStruct(result, source);
        break;
      default:
        throw new IllegalArgumentException("Unknown type " +
            result.getCategory() + " at " + source);
    }
    return result;
  }

  /**
   * Split a compound name into parts separated by '.'.
   * @param source the string to parse into simple names
   * @return a list of simple names from the source
   */
  private static List<String> splitName(ParserUtils.StringPosition source) {
    List<String> result = new ArrayList<>();
    do {
      result.add(parseName(source));
    } while (consumeChar(source, '.'));
    return result;
  }


  private static final Pattern INTEGER_PATTERN = Pattern.compile("^[0-9]+$");

  public static TypeDescription findSubtype(TypeDescription schema,
                                            ParserUtils.StringPosition source) {
    return findSubtype(schema, source, true);
  }

  public static TypeDescription findSubtype(TypeDescription schema,
                                            ParserUtils.StringPosition source,
                                            boolean isSchemaEvolutionCaseAware) {
    List<String> names = ParserUtils.splitName(source);
    if (names.size() == 1 && INTEGER_PATTERN.matcher(names.get(0)).matches()) {
      return schema.findSubtype(Integer.parseInt(names.get(0)));
    }
    TypeDescription current = SchemaEvolution.checkAcidSchema(schema)
        ? SchemaEvolution.getBaseRow(schema) : schema;
    while (names.size() > 0) {
      String first = names.remove(0);
      switch (current.getCategory()) {
        case STRUCT: {
          int posn = -1;
          if (isSchemaEvolutionCaseAware) {
            posn = current.getFieldNames().indexOf(first);
          } else {
            // Case-insensitive search like ORC 1.5
            for (int i = 0; i < current.getFieldNames().size(); i++) {
              if (current.getFieldNames().get(i).equalsIgnoreCase(first)) {
                posn = i;
                break;
              }
            }
          }
          if (posn == -1) {
            throw new IllegalArgumentException("Field " + first +
                " not found in " + current.toString());
          }
          current = current.getChildren().get(posn);
          break;
        }
        case LIST:
          if (first.equals("_elem")) {
            current = current.getChildren().get(0);
          } else {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString());
          }
          break;
        case MAP:
          if (first.equals("_key")) {
            current = current.getChildren().get(0);
          } else if (first.equals("_value")) {
            current = current.getChildren().get(1);
          } else {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString());
          }
          break;
        case UNION: {
          try {
            int posn = Integer.parseInt(first);
            if (posn < 0 || posn >= current.getChildren().size()) {
              throw new NumberFormatException("off end of union");
            }
            current = current.getChildren().get(posn);
          } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Field " + first +
                "not found in " + current.toString(), e);
          }
          break;
        }
        default:
          throw new IllegalArgumentException("Field " + first +
              "not found in " + current.toString());
      }
    }
    return current;
  }

  public static List<TypeDescription> findSubtypeList(TypeDescription schema,
                                                      StringPosition source) {
    List<TypeDescription> result = new ArrayList<>();
    if (source.hasCharactersLeft()) {
      do {
        result.add(findSubtype(schema, source));
      } while (consumeChar(source, ','));
    }
    return result;
  }

  public static class StringPosition {
    final String value;
    int position;
    final int length;

    public StringPosition(String value) {
      this.value = value == null ? "" : value;
      position = 0;
      length = this.value.length();
    }

    @Override
    public String toString() {
      return '\'' + value.substring(0, position) + '^' +
          value.substring(position) + '\'';
    }

    public String fromPosition(int start) {
      return value.substring(start, this.position);
    }

    public boolean hasCharactersLeft() {
      return position != length;
    }
  }

  /**
   * Annotate the given schema with the encryption information.
   *
   * Format of the string is a key-list.
   * <ul>
   *   <li>key-list = key (';' key-list)?</li>
   *   <li>key = key-name ':' field-list</li>
   *   <li>field-list = field-name ( ',' field-list )?</li>
   *   <li>field-name = number | field-part ('.' field-name)?</li>
   *   <li>field-part = quoted string | simple name</li>
   * </ul>
   *
   * @param source the string to parse
   * @param schema the top level schema
   * @throws IllegalArgumentException if there are conflicting keys for a field
   */
  public static void parseKeys(StringPosition source, TypeDescription schema) {
    if (source.hasCharactersLeft()) {
      do {
        String keyName = parseName(source);
        requireChar(source, ':');
        for (TypeDescription field : findSubtypeList(schema, source)) {
          String prev = field.getAttributeValue(TypeDescription.ENCRYPT_ATTRIBUTE);
          if (prev != null && !prev.equals(keyName)) {
            throw new IllegalArgumentException("Conflicting encryption keys " +
                keyName + " and " + prev);
          }
          field.setAttribute(TypeDescription.ENCRYPT_ATTRIBUTE, keyName);
        }
      } while (consumeChar(source, ';'));
    }
  }

  /**
   * Annotate the given schema with the masking information.
   *
   * Format of the string is a mask-list.
   * <ul>
   *   <li>mask-list = mask (';' mask-list)?</li>
   *   <li>mask = mask-name (',' parameter)* ':' field-list</li>
   *   <li>field-list = field-name ( ',' field-list )?</li>
   *   <li>field-name = number | field-part ('.' field-name)?</li>
   *   <li>field-part = quoted string | simple name</li>
   * </ul>
   *
   * @param source the string to parse
   * @param schema the top level schema
   * @throws IllegalArgumentException if there are conflicting masks for a field
   */
  public static void parseMasks(StringPosition source, TypeDescription schema) {
    if (source.hasCharactersLeft()) {
      do {
        // parse the mask and parameters, but only get the underlying string
        int start = source.position;
        parseName(source);
        while (consumeChar(source, ',')) {
          parseName(source);
        }
        String maskString = source.fromPosition(start);
        requireChar(source, ':');
        for (TypeDescription field : findSubtypeList(schema, source)) {
          String prev = field.getAttributeValue(TypeDescription.MASK_ATTRIBUTE);
          if (prev != null && !prev.equals(maskString)) {
            throw new IllegalArgumentException("Conflicting encryption masks " +
                maskString + " and " + prev);
          }
          field.setAttribute(TypeDescription.MASK_ATTRIBUTE, maskString);
        }
      } while (consumeChar(source, ';'));
    }
  }

  public static MaskDescriptionImpl buildMaskDescription(String value) {
    StringPosition source = new StringPosition(value);
    String maskName = parseName(source);
    List<String> params = new ArrayList<>();
    while (consumeChar(source, ',')) {
      params.add(parseName(source));
    }
    return new MaskDescriptionImpl(maskName,
        params.toArray(new String[params.size()]));
  }
}
