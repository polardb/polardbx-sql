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

package com.alibaba.polardbx.common.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.Base64;

@SuppressWarnings("restriction")
public class SerializeUtils {

    protected static final Logger logger = LoggerFactory.getLogger(SerializeUtils.class);

    public static String getBase64(String s) {
        if (s == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(s.getBytes());
    }

    public static String getBase64(byte[] ba) {
        if (ba == null) {
            return null;
        }
        return Base64.getEncoder().encodeToString(ba);
    }

    public static byte[] base64ToByteArray(String base64) throws IOException {
        if (base64 == null) {
            return null;
        }

        return Base64.getDecoder().decode(base64);
    }

    @SuppressWarnings("unchecked")
    public static <T> T deFromBase64(String base64, Class<T> serializedClass) {
        Object o = null;
        byte[] ba = null;
        ByteArrayInputStream bai = null;
        ObjectInputStream oi = null;
        try {
            ba = SerializeUtils.base64ToByteArray(base64);
            bai = new ByteArrayInputStream(ba);
            oi = new ObjectInputStream(bai);
            o = oi.readObject();
        } catch (IOException e) {
            logger.error("deFromBase64 error:" + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("deFromBase64 error:" + e.getMessage());
        } finally {
            try {
                if (oi != null) {
                    oi.close();
                }
                if (bai != null) {
                    bai.close();
                }
            } catch (IOException e) {
                logger.error("deFromBase64 error:" + e.getMessage());
            }
        }
        return o == null ? null : (T) o;
    }

    public static <T> T deFromBytes(byte[] ba, Class<T> serializedClass) {
        Object o = null;
        ByteArrayInputStream bai = null;
        ObjectInputStream oi = null;
        try {
            bai = new ByteArrayInputStream(ba);
            oi = new ObjectInputStream(bai);
            o = oi.readObject();
        } catch (IOException e) {
            logger.error("deFromBytes error:" + e.getMessage());
        } catch (ClassNotFoundException e) {
            logger.error("deFromBytes error:" + e.getMessage());
        } finally {
            try {
                if (oi != null) {
                    oi.close();
                }
                if (bai != null) {
                    bai.close();
                }
            } catch (IOException e) {
                logger.error("deFromBytes error:" + e.getMessage());
            }
        }
        return o == null ? null : (T) o;
    }

    public static String se2base64(Serializable obj) {
        byte[] ba = getBytes(obj);
        return ba == null ? null : SerializeUtils.getBase64(ba);
    }

    public static byte[] getBytes(Serializable obj) {
        byte[] ba = null;
        ByteArrayOutputStream baos = null;
        ObjectOutputStream oos = null;
        try {
            baos = new ByteArrayOutputStream();
            oos = new ObjectOutputStream(baos);
            oos.writeObject(obj);
            ba = baos.toByteArray();
        } catch (IOException e) {
            logger.error("se2base64 error:" + e.getMessage());
        } finally {
            try {
                if (baos != null) {
                    baos.close();
                }
                if (oos != null) {
                    oos.close();
                }
            } catch (IOException e) {
                logger.error("se2base64 error:" + e.getMessage());
            }
        }
        return ba;
    }
}
