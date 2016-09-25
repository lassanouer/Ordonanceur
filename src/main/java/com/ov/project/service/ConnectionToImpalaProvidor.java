package com.ov.project.service;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import com.ov.project.utilities.Constants;

public class ConnectionToImpalaProvidor {

	public Connection con = null;
	public String host = null;
	//	private String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	private final String JDBC_DRIVER_NAME = "org.apache.hive.jdbc.HiveDriver";
	public ConnectionToImpalaProvidor(String ip,String CONNECTION_URL){
		host = ip;
		try {
			try {
				Class.forName(JDBC_DRIVER_NAME);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			con = DriverManager.getConnection(CONNECTION_URL);
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
}