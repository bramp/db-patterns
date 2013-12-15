package net.bramp.sql;

import java.sql.ResultSet;
import java.sql.SQLException;

public final class ResultSets {
	private ResultSets() {}
	
	public static String toString(ResultSet rs) throws SQLException {
		StringBuilder sb = new StringBuilder();
		int cols = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= cols; i++) {
			sb.append('"').append( rs.getString(i) ).append('"').append( ", ");
		}

		if (cols > 0)
			sb.setLength( sb.length() - 2);

		return sb.toString();
	}
}
