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

package org.apache.orc.impl.writer;

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.orc.TypeDescription;
import org.roaringbitmap.RoaringBitmap;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

public class StringTreeWriter extends StringBaseTreeWriter {
  StringTreeWriter(TypeDescription schema,
                   WriterEncryptionVariant encryption,
                   WriterContext writer) throws IOException {
    super(schema, encryption, writer);
  }

  @Override
  public void writeBatch(ColumnVector vector, int offset,
                         int length) throws IOException {
    super.writeBatch(vector, offset, length);
    BytesColumnVector vec = (BytesColumnVector) vector;
    if (vector.isRepeating) {
      if (vector.noNulls || !vector.isNull[0]) {
        if (useDictionaryEncoding) {
          int id = dictionary.add(vec.vector[0], vec.start[0], vec.length[0]);
          for (int i = 0; i < length; ++i) {
            rows.add(id);
          }
        } else {
          for (int i = 0; i < length; ++i) {
            directStreamOutput.write(vec.vector[0], vec.start[0],
                vec.length[0]);
            lengthOutput.write(vec.length[0]);
          }
        }
        indexStatistics.updateString(vec.vector[0], vec.start[0],
            vec.length[0], length);
        if (createBloomFilter) {
          if (bloomFilter != null) {
            // translate from UTF-8 to the default charset
            bloomFilter.addString(new String(vec.vector[0], vec.start[0],
                vec.length[0], StandardCharsets.UTF_8));
          }
          bloomFilterUtf8.addBytes(vec.vector[0], vec.start[0], vec.length[0]);
        }
      } else {
        //有null值更新
        indexStatistics.updateNull();
      }
    } else {
      for (int i = 0; i < length; ++i) {
        if (vec.noNulls || !vec.isNull[i + offset]) {
          if (useDictionaryEncoding) {
            rows.add(dictionary.add(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]));
          } else {
            directStreamOutput.write(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
            lengthOutput.write(vec.length[offset + i]);
          }
          updateBitmapIndexBuildingFlag();
          if(bitmapIndexBuildingFlag){
            // build bitmap index
            bitmapIndexEntryMap.compute(new String(vec.vector[offset + i]),
                (s, roaringBitmap) -> {
                  RoaringBitmap newVal = RoaringBitmap.bitmapOf(savedRowIndex.size());
                  if (roaringBitmap == null) {
                    return newVal;
                  }
                  return RoaringBitmap.or(roaringBitmap, newVal);
                });
          }

          indexStatistics.updateString(vec.vector[offset + i],
              vec.start[offset + i], vec.length[offset + i], 1);
          if (createBloomFilter) {
            if (bloomFilter != null) {
              // translate from UTF-8 to the default charset
              bloomFilter.addString(new String(vec.vector[offset + i],
                  vec.start[offset + i], vec.length[offset + i],
                  StandardCharsets.UTF_8));
            }
            bloomFilterUtf8.addBytes(vec.vector[offset + i],
                vec.start[offset + i], vec.length[offset + i]);
          }
        } else if (i == 0 || i == length - 1) {
          //只用更新第一行和最后一行为null的情况
          indexStatistics.updateNull();
        }
      }
    }
  }
}
