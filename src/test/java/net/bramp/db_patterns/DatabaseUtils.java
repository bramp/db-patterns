package net.bramp.db_patterns;

import javax.sql.DataSource;

import com.mysql.jdbc.jdbc2.optional.MysqlDataSource;

public class DatabaseUtils {
	public static DataSource createDataSource() {
		MysqlDataSource ds = new MysqlDataSource();
		ds.setUser("root");
		ds.setPassword("sP6prUCe");
		ds.setServerName("localhost");
		return ds;
	}
}
