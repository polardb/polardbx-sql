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
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundValueKind;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SqlCollation;

import java.nio.charset.Charset;
import java.util.Comparator;

/**
 * @author chenghui.lch
 */
public class SearchDatumComparator implements Comparator<SearchDatumInfo> {

    /**
     * The rel type of predicate expr of part key i
     */
    protected RelDataType[] datumRelDataTypes;
    /**
     * The drds return type of predicate expr of part key i
     */
    protected DataType[] datumDrdsDataTypes;

    /**
     * The charset of predicate expr of part key i
     */
    protected Charset[] datumCharsets;
    /**
     * The collation of predicate expr of part key i
     */
    protected SqlCollation[] datumCollations;

    public SearchDatumComparator(RelDataType[] datumDataTypes,
                                 DataType[] datumDrdsDataTypes,
                                 Charset[] datumCharsets,
                                 SqlCollation[] datumCollations) {
        this.datumRelDataTypes = datumDataTypes;
        this.datumDrdsDataTypes = datumDrdsDataTypes;
        this.datumCharsets = datumCharsets;
        this.datumCollations = datumCollations;
    }

    /**
     * if datum1 > datum2: return 1;
     * if datum1 = datum2: return 0;
     * if datum1 < datum2: return -1;
     */
    @Override
    public int compare(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        return compareSearchDatum(datum1, datum2);
    }

    public boolean isGreaterThan(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        return compare(datum1, datum2) == 1;
    }

    public boolean isEqualTo(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        return compare(datum1, datum2) == 0;
    }

    public boolean isLessThan(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        return compare(datum1, datum2) == -1;
    }

    public boolean isGreaterThanOrEqualTo(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        int result = compare(datum1, datum2);
        return result == 1 || result == 0;
    }

    public boolean isLessThanOrEqualTo(SearchDatumInfo datum1, SearchDatumInfo datum2) {
        int result = compare(datum1, datum2);
        return result == 0 || result == -1;
    }

    protected int compareSearchDatum(SearchDatumInfo boundDatum, SearchDatumInfo queryDatum) {

        PartitionBoundVal[] queryDatumVal = queryDatum.datumInfo;
        PartitionBoundVal[] boundDatumVal = boundDatum.datumInfo;
        int queryDatumPrefixLen = queryDatumVal.length;
        int retVal = 0;

        for (int i = 0; i < queryDatumPrefixLen; i++) {
            PartitionBoundValueKind bndValKind = boundDatumVal[i].getValueKind();
            PartitionBoundValueKind queryValKind = queryDatumVal[i].getValueKind();
            if (queryValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                // for queryValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE
                if (bndValKind == PartitionBoundValueKind.DATUM_MIN_VALUE) {
                    return -1;
                } else if (bndValKind == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                    return 1;
                } else if (bndValKind == PartitionBoundValueKind.DATUM_DEFAULT_VALUE) {
                    return 1;
                }

                retVal = doCompareDatumVal(
                    boundDatumVal[i].getValue(),
                    boundDatumVal[i].isNullValue(),
                    queryDatumVal[i].getValue(),
                    queryDatumVal[i].isNullValue());
                if (retVal != 0) {
                    break;
                }
            } else if (queryValKind == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                // queryValKind == PartitionBoundValueKind.DATUM_MAX_VALUE
                if (bndValKind == PartitionBoundValueKind.DATUM_MIN_VALUE) {
                    return -1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                    return -1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                    retVal = 0;
                }
            } else if (queryValKind == PartitionBoundValueKind.DATUM_ANY_VALUE) {
                // queryValKind == PartitionBoundValueKind.DATUM_MAX_VALUE
                if (bndValKind == PartitionBoundValueKind.DATUM_MIN_VALUE) {
                    return -1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                    return -1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                    retVal = 0;
                }
            } else if (queryValKind == PartitionBoundValueKind.DATUM_DEFAULT_VALUE) {
                if (bndValKind == PartitionBoundValueKind.DATUM_DEFAULT_VALUE) {
                    retVal = 0;
                } else {
                    retVal = 1;
                }
            } else {
                // queryValKind == PartitionBoundValueKind.DATUM_MIN_VALUE
                if (bndValKind == PartitionBoundValueKind.DATUM_NORMAL_VALUE) {
                    return 1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_MAX_VALUE) {
                    retVal = 1;
                }
                if (bndValKind == PartitionBoundValueKind.DATUM_MIN_VALUE) {
                    retVal = 0;
                }
                if (retVal != 0) {
                    break;
                }
            }
        }

        return Integer.compare(retVal, 0);
    }

    protected static int doCompareDatumVal(
        Object bndData,
        boolean isBndNullVal,
        Object qryData,
        boolean isQryDataNullVal) {

        if (isBndNullVal) {
            if (isQryDataNullVal) {
                return 0;
            } else {
                return -1;
            }
        } else {
            if (isQryDataNullVal) {
                return 1;
            } else {
                return ((PartitionField) bndData).compareTo((PartitionField) qryData);
            }
        }
    }

    public RelDataType[] getDatumRelDataTypes() {
        return datumRelDataTypes;
    }

    public DataType[] getDatumDrdsDataTypes() {
        return datumDrdsDataTypes;
    }

    public Charset[] getDatumCharsets() {
        return datumCharsets;
    }

    public SqlCollation[] getDatumCollations() {
        return datumCollations;
    }
}
