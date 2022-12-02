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

package com.alibaba.polardbx.executor.partitionvisualizer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import java.util.Base64;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;

/**
 * @author ximing.yd
 * @date 2022/2/8 3:20 下午
 */
public class VisualCompressUtil {

    private static final Logger logger = LoggerFactory.getLogger(VisualCompressUtil.class);

    public static String compress(String str) {
        if (str == null || str.length() == 0) {
            return str;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        GZIPOutputStream gzip = null;
        try {
            gzip = new GZIPOutputStream(out);
            gzip.write(str.getBytes());
        } catch (Exception e) {
            logger.error(String.format("compress str:%s error", str), e);
        } finally {
            if (gzip != null) {
                try {
                    gzip.close();
                } catch (Exception e) {
                    logger.error(String.format("compress gzip close str:%s error", str), e);
                }
            }
        }
        return Base64.getEncoder().encodeToString(out.toByteArray());
    }

    public static String uncompress(String compressedStr) {
        if (compressedStr == null) {
            return null;
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = null;
        GZIPInputStream ginzip = null;
        byte[] compressed;
        String decompressed = null;
        try {
            compressed = Base64.getDecoder().decode(compressedStr);
            in = new ByteArrayInputStream(compressed);
            ginzip = new GZIPInputStream(in);
            byte[] buffer = new byte[1024];
            int offset = -1;
            while ((offset = ginzip.read(buffer)) != -1) {
                out.write(buffer, 0, offset);
            }
            decompressed = out.toString();
        } catch (Exception e) {
            logger.error(String.format("uncompress str:%s error", compressedStr), e);
        } finally {
            if (ginzip != null) {
                try {
                    ginzip.close();
                } catch (Exception e) {
                    logger.error(String.format("uncompress ginzip close str:%s error", compressedStr), e);
                }
            }
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    logger.error(String.format("uncompress  in close str:%s error", compressedStr), e);
                }
            }
            try {
                out.close();
            } catch (Exception e) {
                logger.error(String.format("uncompress out close str:%s error", compressedStr), e);
            }
        }
        return decompressed;
    }
}
