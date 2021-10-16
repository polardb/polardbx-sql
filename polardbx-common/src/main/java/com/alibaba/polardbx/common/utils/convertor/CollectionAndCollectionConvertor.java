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

package com.alibaba.polardbx.common.utils.convertor;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Vector;


public class CollectionAndCollectionConvertor {

    public static abstract class BaseCollectionConvertor extends AbastactConvertor {

        @Override
        public Object convert(Object src, Class destClass) {
            return convertCollection(src, destClass, getComponentClass(src, destClass));
        }

        protected abstract Class getComponentClass(Object src, Class destClass);

        protected MappingConfig initMapping(Class srcClass, Class targetClass, Class[] componentClasses) {
            MappingConfig config = new MappingConfig();

            config.convertor = ConvertorHelper.getInstance().getConvertor(srcClass, targetClass);
            if (config.convertor != null) {
                if (componentClasses != null && componentClasses.length > 1) {
                    Class[] newComponentClasses = new Class[componentClasses.length - 1];
                    config.componentClasses = newComponentClasses;
                    System
                        .arraycopy(componentClasses, 1, newComponentClasses, 0, componentClasses.length - 1);
                }
            }
            return config;
        }

        protected Object doMapping(Object src, Class targetClass, MappingConfig config) {
            Object newObj = null;
            if (config.convertor != null) {
                if (config.componentClasses != null) {
                    newObj = config.convertor.convertCollection(src, targetClass, config.componentClasses);
                } else {
                    newObj = config.convertor.convert(src, targetClass);
                }

            }

            return newObj;
        }

        protected Map createMap(Class destClass) {
            if (destClass == Map.class || destClass == HashMap.class) {
                return new HashMap();
            }

            if (destClass == TreeMap.class) {
                return new TreeMap();
            }

            if (destClass == LinkedHashMap.class) {
                return new LinkedHashMap();
            }

            throw new ConvertorException("Unsupported Map: [" + destClass.getName() + "]");

        }

        protected Collection createCollection(Class destClass) {
            if (destClass == List.class || destClass == ArrayList.class) {
                return new ArrayList();
            }

            if (destClass == LinkedList.class) {
                return new LinkedList();
            }

            if (destClass == Vector.class) {
                return new Vector();
            }

            if (destClass == Set.class || destClass == HashSet.class) {
                return new HashSet();
            }

            if (destClass == LinkedHashSet.class) {
                return new LinkedHashSet();
            }

            if (destClass == TreeSet.class) {
                return new TreeSet();
            }

            throw new ConvertorException("Unsupported Collection: [" + destClass.getName() + "]");
        }

        protected void arraySet(Object src, Class compoentType, int i, Object value) {
            Array.set(src, i, value);
        }

        protected Object arrayGet(Object src, Class compoentType, int i) {
            return Array.get(src, i);
        }
    }


    public static class CollectionToCollection extends BaseCollectionConvertor {

        public Object convertCollection(Object src, Class destClass, Class... componentClasses) {
            if (Collection.class.isAssignableFrom(src.getClass()) && Collection.class
                .isAssignableFrom(destClass)) {
                Collection collection = (Collection) src;
                Collection target = createCollection(destClass);

                boolean isInit = false;
                MappingConfig config = null;

                Class componentClass = null;
                if (componentClasses != null && componentClasses.length >= 1) {
                    componentClass = componentClasses[0];
                }

                for (Iterator iter = collection.iterator(); iter.hasNext(); ) {
                    Object item = iter.next();
                    Class componentSrcClass = item.getClass();

                    if (componentClass != null && componentSrcClass != componentClass) {
                        if (isInit == false) {
                            isInit = true;
                            config = initMapping(componentSrcClass, componentClass, componentClasses);
                        }


                        target.add(doMapping(item, componentClass, config));
                    } else {
                        target.add(item);
                    }
                }
                return target;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        @Override
        protected Class getComponentClass(Object src, Class destClass) {
            if (Collection.class.isAssignableFrom(src.getClass()) && Collection.class
                .isAssignableFrom(destClass)) {
                Collection collection = (Collection) src;
                for (Iterator iter = collection.iterator(); iter.hasNext(); ) {
                    Object item = iter.next();
                    if (item != null) {
                        return item.getClass();
                    }
                }
            }

            return null;
        }
    }


    public static class ArrayToArray extends BaseCollectionConvertor {

        public Object convertCollection(Object src, Class destClass, Class... componentClasses) {
            if (src.getClass().isArray() && destClass.isArray()) {
                int size = Array.getLength(src);
                Class componentSrcClass = src.getClass().getComponentType();
                Class componentDestClass = destClass.getComponentType();
                MappingConfig config = null;
                Object[] objs = (Object[]) Array.newInstance(componentDestClass, size);

                Class componentClass = null;
                if (componentClasses != null && componentClasses.length >= 1) {
                    componentClass = componentClasses[0];
                }

                if (componentDestClass != componentClass) {
                    throw new ConvertorException("error ComponentClasses config for [" + componentDestClass.getName()
                        + "] to [" + componentDestClass.getName() + "]");
                }

                if (componentClass != null && componentSrcClass != componentClass) {
                    config = initMapping(componentSrcClass, componentClass, componentClasses);
                }

                for (int i = 0; i < size; i++) {
                    Object obj = arrayGet(src, componentSrcClass, i);
                    if (config != null) {
                        Object newObj = doMapping(obj, componentDestClass, config);
                        arraySet(objs, componentDestClass, i, newObj);
                    } else {
                        arraySet(objs, componentDestClass, i, obj);
                    }
                }
                return objs;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        @Override
        protected Class getComponentClass(Object src, Class destClass) {
            if (src.getClass().isArray() && destClass.isArray()) {
                return src.getClass().getComponentType();
            }
            return null;
        }
    }


    public static class ArrayToCollection extends BaseCollectionConvertor {

        public Object convertCollection(Object src, Class destClass, Class... componentClasses) {
            if (src.getClass().isArray() && Collection.class.isAssignableFrom(destClass)) {
                Collection target = createCollection(destClass);
                int size = Array.getLength(src);
                Class componentSrcClass = src.getClass().getComponentType();

                MappingConfig config = null;
                Class componentClass = null;
                if (componentClasses != null && componentClasses.length >= 1) {
                    componentClass = componentClasses[0];
                }

                if (componentClass != null && componentSrcClass != componentClass) {
                    config = initMapping(componentSrcClass, componentClass, componentClasses);
                }

                for (int i = 0; i < size; i++) {
                    Object obj = arrayGet(src, componentSrcClass, i);
                    if (config != null) {
                        Object newObj = doMapping(obj, componentClass, config);
                        target.add(newObj);
                    } else {
                        target.add(obj);
                    }
                }
                return target;
            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        @Override
        protected Class getComponentClass(Object src, Class destClass) {
            if (src.getClass().isArray() && Collection.class.isAssignableFrom(destClass)) {
                return src.getClass().getComponentType();
            }

            return null;
        }

    }

    public static class CollectionToArray extends BaseCollectionConvertor {

        public Object convertCollection(Object src, Class destClass, Class... componentClasses) {
            if (Collection.class.isAssignableFrom(src.getClass()) && destClass.isArray()) {
                Collection collection = (Collection) src;
                Class componentDestClass = destClass.getComponentType();
                Object objs = Array.newInstance(componentDestClass, collection.size());

                boolean isInit = false;
                MappingConfig config = null;
                Class componentClass = null;
                if (componentClasses != null && componentClasses.length >= 1) {
                    componentClass = componentClasses[0];
                }

                if (componentDestClass != componentClass) {
                    throw new ConvertorException("error ComponentClasses config for [" + componentDestClass.getName()
                        + "] to [" + componentDestClass.getName() + "]");
                }

                int i = 0;
                for (Iterator iter = collection.iterator(); iter.hasNext(); ) {
                    Object item = iter.next();
                    Class componentSrcClass = item.getClass();
                    if (componentClass != null && componentSrcClass != componentDestClass) {
                        if (isInit == false) {
                            config = initMapping(componentSrcClass, componentDestClass, componentClasses);
                        }

                        Object newObj = doMapping(item, componentDestClass, config);
                        arraySet(objs, componentDestClass, i, newObj);
                    } else {
                        arraySet(objs, componentDestClass, i, item);
                    }
                    i = i + 1;
                }
                return objs;

            }

            throw new ConvertorException("Unsupported convert: [" + src + "," + destClass.getName() + "]");
        }

        @Override
        protected Class getComponentClass(Object src, Class destClass) {
            if (Collection.class.isAssignableFrom(src.getClass()) && destClass.isArray()) {
                return destClass.getComponentType();
            }

            return null;
        }
    }

}

class MappingConfig {

    Convertor convertor = null;
    Class[] componentClasses = null;

}
