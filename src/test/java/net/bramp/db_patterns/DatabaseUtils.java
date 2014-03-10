package net.bramp.db_patterns;

import javax.sql.DataSource;

import com.google.common.base.Throwables;
import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class DatabaseUtils {
	public static DataSource createDataSource() {
		MysqlDataSource ds = new MysqlDataSource();
		ds.setUser("root");
		ds.setPassword("sP6prUCe");
		ds.setServerName("localhost");
		ds.setDatabaseName("db_patterns");
		return ds;
	}

    public static String getHostname() {
        try {
            return InetAddress.getLocalHost().getHostName();
        } catch (UnknownHostException e) {
            throw Throwables.propagate(e);
        }
    }
}
