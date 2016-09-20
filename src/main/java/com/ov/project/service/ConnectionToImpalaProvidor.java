package com.ov.project.service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ConnectionToImpalaProvidor {

	public Connection mConx = null;
	// private String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";

	public ConnectionToImpalaProvidor(String iCONNECTION_URL) {
		ResultSet rs = null;
		try {
			// Class.forName(JDBC_DRIVER_NAME);
			// ImpalaConnection im_cnn = connections.GetConnectionInstance();
			// con = im_cnn.con;
			// Statement stmt = con.createStatement();
			// System.out.println("\n== Host " + im_cnn.host);
			// rs = stmt.executeQuery(SQL_STATEMENT);
			// System.out.println("\n== Begin Query Results
			// ======================");
			// // print the results to the console
			while (rs.next()) {
				// the example query returns one String column
				System.out.println(rs.getString(1));
			}
			System.out.println("== End Query Results =======================\n\n");

		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}