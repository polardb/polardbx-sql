package com.alibaba.polardbx.transfer.config;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;

/**
 * @author wuzhe
 */
public class CmdOptions {
    private static final Options OPTIONS = new Options();

    static {
        OPTIONS.addOption(Option.builder("config")
            .required()
            .hasArg()
            .desc("config file, in TOML format")
            .build());
        OPTIONS.addOption(Option.builder("op")
            .hasArg()
            .desc("operation type, prepare or run")
            .build());
    }

    public static Options getOptions() {
        return OPTIONS;
    }
}
