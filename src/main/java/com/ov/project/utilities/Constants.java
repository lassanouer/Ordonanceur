package com.ov.project.utilities;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
public class Constants {

	// Config Spark Context
	public static final String sFormatJson = "json";
	public static final String sFormatParquet = "parquet";
	public static final int sPort = 2015;
	public static final String sHost = "127.0.0.1";

	// impala Config
	public static final String sConnection_URL_Property = "connection.url";
	public static final String sJDBC_Driver_Name_Property = "jdbc.driver.class.name";
	
	// Chrones
	public static final int sDeuxMinutes = 120000;
	public static final int sCinqMinutes = 300000;
	public static final int sDixMinutes = 600000;
}
