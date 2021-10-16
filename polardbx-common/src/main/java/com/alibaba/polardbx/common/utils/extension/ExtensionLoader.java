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

package com.alibaba.polardbx.common.utils.extension;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;

import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;

import com.google.common.collect.Maps;


public class ExtensionLoader<S> {

    private static final Logger logger = LoggerFactory.getLogger(ExtensionLoader.class);
    private static final String SERVICES_DIRECTORY = "META-INF/services/";
    private static final String POLARDBX_DIRECTORY = "META-INF/polardbx/";
    private static final String APP_DIRECTORY = "META-INF/app/";
    private static Map<Class, List<Class>> providers = Maps.newConcurrentMap();
    private static Map<String, List<Class>> activateProviders = Maps.newConcurrentMap();


    public static <S> S load(Class<S> service, ClassLoader loader) throws ExtensionNotFoundException {
        return loadFile(service, null, loader);
    }


    public static <S> S load(Class<S> service) throws ExtensionNotFoundException {
        return loadFile(service, null, findClassLoader());
    }

    public static <S> S load(Class<S> service, String activateName) throws ExtensionNotFoundException {
        return loadFile(service, activateName, findClassLoader());
    }

    public static <S> S load(Class<S> service, String activateName, ClassLoader loader)
        throws ExtensionNotFoundException {
        return loadFile(service, activateName, loader);
    }

    public static <S> List<Class> getAllExtendsionClass(Class<S> service) {
        return findAllExtensionClass(service, findClassLoader());
    }

    public static <S> List<Class> getAllExtendsionClass(Class<S> service, ClassLoader loader) {
        return findAllExtensionClass(service, loader);
    }

    private static <S> S loadFile(Class<S> service, String activateName, ClassLoader loader) {
        try {
            boolean foundFromCache = true;
            List<Class> extensions = null;
            if (StringUtils.isNotEmpty(activateName)) {
                String name = service.getName() + "^" + activateName;
                extensions = activateProviders.get(name);
                if (extensions == null) {
                    synchronized (service) {
                        extensions = activateProviders.get(name);
                        if (extensions == null) {
                            extensions = findAlltivateExtensionClass(service, activateName, loader, extensions);
                            foundFromCache = false;
                            activateProviders.put(name, extensions);
                        }
                    }
                }
            } else {
                extensions = providers.get(service);
                if (extensions == null) {
                    synchronized (service) {
                        extensions = providers.get(service);
                        if (extensions == null) {
                            extensions = findAllExtensionClass(service, loader);
                            foundFromCache = false;
                            providers.put(service, extensions);
                        }
                    }
                }
            }

            if (extensions.isEmpty()) {
                throw new ExtensionNotFoundException("not found service provider for : " + service.getName() + "["
                    + activateName + "] and classloader : "
                    + ObjectUtils.toString(loader));
            }
            Class<?> extension = extensions.get(extensions.size() - 1);
            S result = service.cast(extension.newInstance());
            if (!foundFromCache && logger.isInfoEnabled()) {
                logger.info("load " + service.getSimpleName() + "[" + activateName + "] extension by class["
                    + extension.getName() + "]");
            }
            return result;
        } catch (Throwable e) {
            if (e instanceof ExtensionNotFoundException) {
                throw (ExtensionNotFoundException) e;
            } else {
                throw new ExtensionNotFoundException("not found service provider for : " + service.getName()
                    + " caused by " + ExceptionUtils.getFullStackTrace(e));
            }
        }
    }

    private static <S> List<Class> findAlltivateExtensionClass(Class<S> service, String activateName,
                                                               ClassLoader loader, List<Class> extensions)
        throws IOException {
        extensions = providers.get(service);
        if (extensions == null) {
            synchronized (service) {
                extensions = providers.get(service);
                if (extensions == null) {
                    extensions = findAllExtensionClass(service, loader);
                    providers.put(service, extensions);
                }
            }
        }

        if (StringUtils.isNotEmpty(activateName)) {
            loadFile(service, POLARDBX_DIRECTORY + activateName.toLowerCase() + "/", loader, extensions);

            List<Class> activateExtensions = new ArrayList<Class>();
            for (int i = 0; i < extensions.size(); i++) {
                Class clz = extensions.get(i);
                Activate activate = (Activate) clz.getAnnotation(Activate.class);
                if (activate != null && activateName.equals(activate.name())) {
                    activateExtensions.add(clz);
                }
            }
            sort(extensions);
            extensions = activateExtensions;

            sort(extensions);
        }
        return extensions;
    }

    private static <S> List<Class> findAllExtensionClass(Class<S> service, ClassLoader loader) {
        List<Class> extensions = Collections.synchronizedList(new ArrayList<Class>());
        try {
            loadFile(service, SERVICES_DIRECTORY, loader, extensions);
            loadFile(service, POLARDBX_DIRECTORY, loader, extensions);
            loadFile(service, APP_DIRECTORY, loader, extensions);
        } catch (IOException e) {
            throw GeneralUtil.nestedException(e);
        }

        if (extensions.isEmpty()) {
            return extensions;
        }

        sort(extensions);
        return extensions;
    }

    private static void sort(List<Class> extensions) {
        Collections.sort(extensions, new Comparator<Class>() {

            @Override
            public int compare(Class c1, Class c2) {
                Integer o1 = 0;
                Integer o2 = 0;
                Activate a1 = (Activate) c1.getAnnotation(Activate.class);
                Activate a2 = (Activate) c2.getAnnotation(Activate.class);

                if (a1 != null) {
                    o1 = a1.order();
                }

                if (a2 != null) {
                    o2 = a2.order();
                }

                return o1.compareTo(o2);

            }
        });
    }

    private static void loadFile(Class<?> service, String dir, ClassLoader classLoader, List<Class> extensions)
        throws IOException {
        String fileName = dir + service.getName();
        Enumeration<java.net.URL> urls;
        if (classLoader != null) {
            urls = classLoader.getResources(fileName);
        } else {
            urls = ClassLoader.getSystemResources(fileName);
        }

        if (urls != null) {
            while (urls.hasMoreElements()) {
                java.net.URL url = urls.nextElement();
                BufferedReader reader = null;
                try {
                    reader = new BufferedReader(new InputStreamReader(url.openStream(), "utf-8"));
                    String line = null;
                    while ((line = reader.readLine()) != null) {
                        final int ci = line.indexOf('#');
                        if (ci >= 0) {
                            line = line.substring(0, ci);
                        }
                        line = line.trim();
                        if (line.length() > 0) {
                            extensions.add(Class.forName(line, true, classLoader));
                        }
                    }
                } catch (ClassNotFoundException e) {

                } catch (Throwable e) {
                    logger.warn(e);
                } finally {
                    IOUtils.closeQuietly(reader);
                }
            }
        }
    }

    private static ClassLoader findClassLoader() {

        return ExtensionLoader.class.getClassLoader();
    }

}
