package com.alibaba.polardbx.transfer.config;

import com.moandjiezana.toml.Toml;

import java.io.File;

/**
 * @author wuzhe
 */
public class TomlConfig {
    private static final TomlConfig INSTANCE = new TomlConfig();

    public static Toml config;

    private TomlConfig() {
    }

    public static TomlConfig getInstance() {
        return INSTANCE;
    }

    public static Toml getConfig() {
        return config;
    }

    public void init(String filePath) {
        config = new Toml().read(new File(filePath));
    }

}
