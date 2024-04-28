package com.alibaba.polardbx.transfer;

import com.alibaba.polardbx.transfer.config.CmdOptions;
import com.alibaba.polardbx.transfer.config.TomlConfig;
import com.alibaba.polardbx.transfer.plugin.BasePlugin;
import com.alibaba.polardbx.transfer.plugin.IPlugin;
import com.alibaba.polardbx.transfer.utils.Utils;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * @author wuzhe
 */
public class Runner {
    private static final Logger logger = LoggerFactory.getLogger(Runner.class);

    public static void main(String[] args) throws Exception {
        boolean prepare = loadConfig(args);

        if (prepare) {
            Utils.prepare();
            logger.warn("Prepare done.");
            return;
        }

        List<IPlugin> plugins = loadPlugins();
        for (IPlugin plugin : plugins) {
            plugin.run();
        }

        Thread monitorThread = new Thread(() -> monitor(plugins), "Monitor");
        monitorThread.start();

        long timeout = TomlConfig.getConfig().getLong("timeout");
        try {
            BasePlugin.waitUtilTimeout(timeout * 1000);
        } catch (Throwable t) {
            logger.warn("Error", t);
        }

        monitorThread.interrupt();
        for (IPlugin plugin : plugins) {
            plugin.interrupt();
        }

        logger.warn("Done.");
    }

    private static boolean loadConfig(String[] args) {
        // Get config file path.
        String configFilePath;
        boolean prepare = false;
        try {
            CommandLineParser parser = new DefaultParser();
            CommandLine cmd = parser.parse(CmdOptions.getOptions(), args);
            configFilePath = cmd.getOptionValue("config");
            prepare = "prepare".equalsIgnoreCase(cmd.getOptionValue("op", "run"));
        } catch (Throwable t) {
            logger.warn("Cant parse config file path in cmd line, use default config.toml .");
            configFilePath = "config.toml";
        }

        // Read config from file.
        TomlConfig.getInstance().init(configFilePath);
        return prepare;
    }

    private static List<IPlugin> loadPlugins() {
        List<IPlugin> plugins = new ArrayList<>();
        String pluginPackage = "com.alibaba.polardbx.transfer.plugin";
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        List<String> classNames;
        if ("local".equalsIgnoreCase(TomlConfig.getConfig().getString("runmode"))) {
            classNames = Utils.getClassName(pluginPackage, classLoader);
        } else {
            classNames = Utils.getClassNameFromJar(pluginPackage);
        }
        logger.debug("Number of classes found: " + classNames.size());
        Collections.sort(classNames);
        for (String className : classNames) {
            if (!className.endsWith("Plugin")) {
                continue;
            }
            logger.debug("Initializing plugin: " + className);
            try {
                Class<?> clazz = classLoader.loadClass(className);
                if (clazz.isInterface() || java.lang.reflect.Modifier.isAbstract(clazz.getModifiers())) {
                    // ignore interface and abstract class
                    continue;
                }
                Constructor<?> constructor = clazz.getConstructor();
                IPlugin plugin = (IPlugin) constructor.newInstance();
                if (plugin.isEnabled()) {
                    plugins.add(plugin);
                }
            } catch (Throwable t) {
                logger.warn("Find plugins error", t);
            }
        }
        return plugins;
    }

    private static void monitor(Collection<IPlugin> plugins) {
        long reportInterval = TomlConfig.getConfig().getLong("report_interval");

        try {
            do {
                Thread.sleep(reportInterval * 1000);

                logger.warn("TPS:");
                for (IPlugin plugin : plugins) {
                    String pluginName = plugin.getClass().getSimpleName();
                    long finishOps = plugin.getOpAndClear();
                    double tps = 1.0 * finishOps / reportInterval;
                    logger.warn(String.format("%-30s %.1f", pluginName, tps));
                }

            } while (true);
        } catch (InterruptedException e) {
            // ignore
        }
    }
}
