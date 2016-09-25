package com.ov.project.commons;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import com.ov.project.utilities.BundelUtils;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
// Pas de synchronized pour des raisons de performance
public class SingletonWrappers {

	private static JavaSparkContext mInstanceJavaSparkContext;
	private static SparkConf mInstanceSparkConf;
	private static Connection mInstanceConnection;

	private SingletonWrappers() {
	}

	public static JavaSparkContext sparkContextGetInstance() {
		if (mInstanceJavaSparkContext == null) {
			mInstanceSparkConf = sparkConfGetInstance();
			mInstanceJavaSparkContext = new JavaSparkContext(mInstanceSparkConf);
		}
		return mInstanceJavaSparkContext;
	}

	public static SparkConf sparkConfGetInstance() {
		if (mInstanceSparkConf == null) {
			mInstanceSparkConf = new SparkConf().setAppName("OV Project").setMaster("local");
		}
		return mInstanceSparkConf;
	}

	public static Connection impalaConnectionGetInstance() {
		if (mInstanceConnection == null) {
			try {
				Class.forName(BundelUtils.get("jdbc.driver.class.name"));
				mInstanceConnection = DriverManager.getConnection(BundelUtils.get("connection.url"));
			} catch (SQLException | ClassNotFoundException e) {
				e.printStackTrace();
			}
		}
		return mInstanceConnection;
	}

}
