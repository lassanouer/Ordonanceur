package com.ov.project.dev.crawler;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class TestConnectImpala {
	public void testImpala() {
		try {
			Class.forName("com.cloudera.impala.jdbc4.Driver");
			Connection connection = null;
			connection = DriverManager.getConnection("jdbc:impala://localhost:21050/ovproject");
			Statement statement = null;
			statement = connection.createStatement();
			ResultSet resultSet = statement.executeQuery("select * from stations;");
			while (resultSet.next()) {
				System.out.println(resultSet.getInt(0));
			}
		} catch (SQLException | ClassNotFoundException e) {
			e.printStackTrace();
		}
	}
}
