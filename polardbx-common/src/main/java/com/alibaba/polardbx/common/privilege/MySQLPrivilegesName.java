package com.alibaba.polardbx.common.privilege;

public enum MySQLPrivilegesName {
    ALTER("Alter", "Tables", "To alter the table"),
    ALTER_ROUTINE("Alter routine", "Functions,Procedures", "To alter or drop stored functions/procedures"),
    CREATE("Create", "Databases,Tables,Indexes", "To create new databases and tables"),
    CREATE_ROUTINE("Create routine", "Databases", "To use CREATE FUNCTION/PROCEDURE"),
    CREATE_TEMPORARY_TABLES("Create temporary tables", "Databases", "To use CREATE TEMPORARY TABLE"),
    CREATE_VIEW("Create view", "Tables", "To create new views"),
    CREATE_USER("Create user", "Server Admin", "To create new users"),
    DELETE("Delete", "Tables", "To delete existing rows"),
    DROP("Drop", "Databases,Tables", "To drop databases, tables, and views"),
    EVENT("Event", "Server Admin", "To create, alter, drop and execute events"),
    EXECUTE("Execute", "Functions,Procedures", "To execute stored routines"),
    FILE("File", "File access on server", "To read and write files on the server"),
    GRANT_OPTION("Grant option", "Databases,Tables,Functions,Procedures",
        "To give to other users those privileges you possess"),
    INDEX("Index", "Tables", "To create or drop indexes"),
    INSERT("Insert", "Tables", "To insert data into tables"),
    LOCK_TABLES("Lock tables", "Databases", "To use LOCK TABLES (together with SELECT privilege)"),
    PROCESS("Process", "Server Admin", "To view the plain text of currently executing queries"),
    PROXY("Proxy", "Server Admin", "To make proxy user possible"),
    REFERENCES("References", "Databases,Tables", "To have references on tables"),
    RELOAD("Reload", "Server Admin", "To reload or refresh tables, logs and privileges"),
    REPLICATION_CLIENT("Replication client", "Server Admin", "To ask where the slave or master servers are"),
    REPLICATION_SLAVE("Replication slave", "Server Admin", "To read binary log events from the master"),
    SELECT("Select", "Tables", "To retrieve rows from table"),
    SHOW_DATABASES("Show databases", "Server Admin", "To see all databases with SHOW DATABASES"),
    SHOW_VIEW("Show view", "Tables", "To see views with SHOW CREATE VIEW"),
    SHUTDOWN("Shutdown", "Server Admin", "To shut down the server"),
    SUPER("Super", "Server Admin", "To use KILL thread, SET GLOBAL, CHANGE MASTER, etc."),
    TRIGGER("Trigger", "Tables", "To use triggers"),
    CREATE_TABLESPACE("Create tablespace", "Server Admin", "To create/alter/drop tablespaces"),
    UPDATE("Update", "Tables", "To update existing rows"),
    USAGE("Usage", "Server Admin", "No privileges - allow connect only");

    private final String privilege;
    private final String context;
    private final String comment;

    MySQLPrivilegesName(String privilege, String context, String comment) {
        this.privilege = privilege;
        this.context = context;
        this.comment = comment;
    }

    public String getPrivilege() {
        return privilege;
    }

    public String getContext() {
        return context;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public String toString() {
        return "DatabasePrivilege{" +
            "privilege='" + privilege + '\'' +
            ", context='" + context + '\'' +
            ", comment='" + comment + '\'' +
            '}';
    }
}