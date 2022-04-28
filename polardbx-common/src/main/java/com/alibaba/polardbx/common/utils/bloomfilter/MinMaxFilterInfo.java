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

package com.alibaba.polardbx.common.utils.bloomfilter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * @author chenzilin
 * @date 2021/12/8 16:09
 */
public class MinMaxFilterInfo {

    public enum TYPE {
        NULL,
        INTEGER,
        LONG,
        STRING,
        TIMESTAMP,
        DATE,
        DECIMAL,
        DOUBLE,
        FLOAT
    }

    private TYPE type;

    private Long minNumber;
    private Long maxNumber;

    private String minString;
    private String maxString;

    private Double minDouble;
    private Double maxDouble;

    private Float minFloat;
    private Float maxFloat;

    @JsonCreator
    public MinMaxFilterInfo(@JsonProperty("type") TYPE type,
                            @JsonProperty("minNumber") Long minNumber,
                            @JsonProperty("maxNumber") Long maxNumber,
                            @JsonProperty("minString") String minString,
                            @JsonProperty("maxString") String maxString,
                            @JsonProperty("minDouble") Double minDouble,
                            @JsonProperty("maxDouble") Double maxDouble,
                            @JsonProperty("minFloat") Float minFloat,
                            @JsonProperty("maxFloat") Float maxFloat) {
        this.type = type;
        this.minNumber = minNumber;
        this.maxNumber = maxNumber;
        this.minString = minString;
        this.maxString = maxString;
        this.minDouble = minDouble;
        this.maxDouble = maxDouble;
        this.minFloat = minFloat;
        this.maxFloat = maxFloat;
    }

    public void merge(MinMaxFilterInfo other) {
        if (minNumber == null) {
            minNumber = other.minNumber;
        } else if (other.minNumber != null) {
            minNumber = Math.min(minNumber, other.minNumber);
        }

        if (maxNumber == null) {
            maxNumber = other.maxNumber;
        } else if (other.maxNumber != null) {
            maxNumber = Math.max(maxNumber, other.maxNumber);
        }

        if (minString == null) {
            minString = other.minString;
        } else if (other.minString != null) {
            minString = minString.compareTo(other.minString) <= 0 ? minString : other.minString;
        }

        if (maxString == null) {
            maxString = other.maxString;
        } else if (other.maxString != null) {
            maxString = maxString.compareTo(other.maxString) >= 0 ? maxString : other.maxString;
        }

        if (minDouble == null) {
            minDouble = other.minDouble;
        } else if (other.minDouble != null) {
            minDouble = Math.min(minDouble, other.minDouble);
        }

        if (maxDouble == null) {
            maxDouble = other.maxDouble;
        } else if (other.maxDouble != null) {
            maxDouble = Math.max(maxDouble, other.maxDouble);
        }

        if (minFloat == null) {
            minFloat = other.minFloat;
        } else if (other.minFloat != null) {
            minFloat = Math.min(minFloat, other.minFloat);
        }

        if (maxFloat == null) {
            maxFloat = other.maxFloat;
        } else if (other.maxFloat != null) {
            maxFloat = Math.max(maxFloat, other.maxFloat);
        }
    }

    @JsonProperty
    public Long getMinNumber() {
        return minNumber;
    }

    @JsonProperty
    public Long getMaxNumber() {
        return maxNumber;
    }

    @JsonProperty
    public String getMinString() {
        return minString;
    }

    @JsonProperty
    public String getMaxString() {
        return maxString;
    }

    @JsonProperty
    public Double getMinDouble() {
        return minDouble;
    }

    @JsonProperty
    public Double getMaxDouble() {
        return maxDouble;
    }

    @JsonProperty
    public Float getMinFloat() {
        return minFloat;
    }

    @JsonProperty
    public Float getMaxFloat() {
        return maxFloat;
    }

    @JsonProperty
    public TYPE getType() {
        return type;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        return Objects.equals(type, ((MinMaxFilterInfo) o).type) &&
                Objects.equals(minNumber, ((MinMaxFilterInfo) o).minNumber) &&
                Objects.equals(maxNumber, ((MinMaxFilterInfo) o).maxNumber) &&
                Objects.equals(minString, ((MinMaxFilterInfo) o).minString) &&
                Objects.equals(maxString, ((MinMaxFilterInfo) o).maxString) &&
                Objects.equals(minDouble, ((MinMaxFilterInfo) o).minDouble) &&
                Objects.equals(maxDouble, ((MinMaxFilterInfo) o).maxDouble) &&
                Objects.equals(minFloat, ((MinMaxFilterInfo) o).minFloat) &&
                Objects.equals(maxFloat, ((MinMaxFilterInfo) o).maxFloat);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, minNumber, maxNumber, minString, maxString, minDouble, maxDouble, minFloat, maxFloat);
    }
}
