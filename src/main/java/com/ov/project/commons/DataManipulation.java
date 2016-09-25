package com.ov.project.commons;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.ov.SparkManager;
import com.ov.importDataSources.ImportAPIfile;
import com.ov.project.mapper.StationDTO;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.DateUtils;

/**
 * 
 * @author Anouer.Lassoued
 *
 *         Manipulation des données depuis l'extraction de Json de l'API &&
 *         Generation de parquets
 *
 */
public class DataManipulation {

	// Date format
	public static SimpleDateFormat sDateAndTime = new SimpleDateFormat(BundelUtils.get("date.and.time.format"));
	public static SimpleDateFormat sDate = new SimpleDateFormat(BundelUtils.get("date.format"));

	/**
	 * Convertir ligne json to StationDTO object
	 * 
	 * @param iLigneJson
	 * @return
	 */
	public static StationDTO jsonStationToObject(List<String> iLigneJson) {
		Date lNow = new Date();
		StationDTO lStation = new StationDTO();

		lStation.setfStationId(
				iLigneJson.get(0).substring(iLigneJson.get(0).indexOf(":") + 1, iLigneJson.get(0).length()));
		lStation.setfBanking(Boolean
				.valueOf(iLigneJson.get(5).substring(iLigneJson.get(5).indexOf(":") + 1, iLigneJson.get(5).length()))
				.booleanValue());
		lStation.setfBonus(Boolean
				.valueOf(iLigneJson.get(6).substring(iLigneJson.get(6).indexOf(":") + 1, iLigneJson.get(6).length()))
				.booleanValue());
		lStation.setfStatus(
				iLigneJson.get(7).substring(iLigneJson.get(7).indexOf(":") + 1, iLigneJson.get(7).length()));
		lStation.setfBikeStands(Float
				.valueOf(iLigneJson.get(9).substring(iLigneJson.get(9).indexOf(":") + 1, iLigneJson.get(9).length()))
				.floatValue());
		lStation.setfAvailableBikeStands(Float
				.valueOf(iLigneJson.get(10).substring(iLigneJson.get(10).indexOf(":") + 1, iLigneJson.get(10).length()))
				.floatValue());
		lStation.setfAvailableBikes(Float
				.valueOf(iLigneJson.get(11).substring(iLigneJson.get(11).indexOf(":") + 1, iLigneJson.get(11).length()))
				.floatValue());

		lStation.setfMonth(String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1));
		lStation.setfDayOfWeek(String.valueOf(Calendar.getInstance().get(Calendar.DAY_OF_WEEK)));
		lStation.setfHour(String.valueOf(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)));
		lStation.setfRoundedMinutes(String.valueOf(Calendar.getInstance().get(Calendar.MINUTE)));

		lStation.setfSystemDate(new Timestamp(lNow.getTime()));
		lStation.setfRealDate(new Timestamp(lNow.getTime()));

		// dates traitées selon le parquet modele
		lStation.setfRoundedSystemDate(new Timestamp(DateUtils.roundFiveMin(lNow).getTime()));
		lStation.setfLaggedRoundedSystemDate(DateUtils.calculateLaggedRoundedSystemDate());

		return lStation;
	}

	/**
	 * recolter les stations de fichier JSON
	 * 
	 * @param iFilename
	 * @return
	 */
	private static JavaRDD<StationDTO> stationDTOFromJsonConverter(String iFilename) {
		JavaSparkContext lJavaSparkContext = SingletonWrappers.sparkContextGetInstance();
		JavaRDD<StationDTO> lStations = lJavaSparkContext.textFile(iFilename).map(line -> {

			// clean text et attributs
			List<String> lUnits = Arrays.asList(line.replace("[{}[]]+", "").split(",\""));
			for (String lTemp : lUnits) {
				lTemp = lTemp.replace("\"", "");
			}

			// retour l'objet stationDTO
			return jsonStationToObject(lUnits);
		});
		return lStations;
	}

	/**
	 * Save Json file to HDFS, Chake Hue
	 * 
	 * @param iFolder
	 * @throws IOException
	 */
	public static void saveJsonIntoHDFS(String iFolder) throws IOException {
		FileSystem lhdfs = FileSystem.get(new Configuration());

		org.apache.hadoop.fs.Path lWorkingDir = lhdfs.getWorkingDirectory();
		org.apache.hadoop.fs.Path lNewFolderPath = new org.apache.hadoop.fs.Path(
				BundelUtils.get("hdfs.bruteData.path") + iFolder);

		lNewFolderPath = org.apache.hadoop.fs.Path.mergePaths(lWorkingDir, lNewFolderPath);

		if (!lhdfs.exists(lNewFolderPath)) {
			lhdfs.mkdirs(lNewFolderPath);
		}

		org.apache.hadoop.fs.Path localFilePath = new org.apache.hadoop.fs.Path(iFolder);
		lhdfs.copyFromLocalFile(localFilePath, lNewFolderPath);
	}

	/**
	 * reception de Json a partir de Url de JcDecaux
	 * 
	 * @param iUrl
	 *            (BundelUtils.get("url.stations"))
	 * @param iPath
	 *            (BundelUtils.get("bruteData.path"))
	 * @return
	 */
	public static String getDataBrute(String iUrl, String iPath) {
		Date lNow = new Date();
		String lFile = BundelUtils.get("suffix.for.data.file") + sDateAndTime.format(lNow) + ".txt";
		Path lPath = Paths.get(iPath + sDate.format(lNow));

		// cree le fichier qui contien les données de la date actuelle
		if (Files.notExists(lPath)) {
			new File(iPath + sDate.format(lNow)).mkdir();
		}

		try {
			ImportAPIfile.storeJSONFileToTxt(iUrl, iPath + sDate.format(lNow), lFile);
			saveJsonIntoHDFS(lFile);
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		// besoin de retourner le file path pour plus simple que l'identifier
		// par date dans la prochaine etape
		return iPath + sDate.format(lNow) + "/" + lFile;
	}

	/**
	 * parser un fichier json a parquets
	 * 
	 * @param iJsonPath
	 *            (BundelUtils.get("bruteData.path"))
	 * @param iParquetPath
	 *            (BundelUtils.get("data.frame.path"))
	 */
	public static void getParquets(String iJsonPath, String iParquetPath) {
		Date lNow = new Date();
		// config
		JavaSparkContext lSparkCtx = SingletonWrappers.sparkContextGetInstance();
		SQLContext lSqlContext = new org.apache.spark.sql.SQLContext(lSparkCtx);

		// get data from Json
		JavaRDD<StationDTO> lStationsDTO = stationDTOFromJsonConverter(iJsonPath);
		DataFrame lSchemaDBrute = lSqlContext.createDataFrame(lStationsDTO, StationDTO.class);

		// join les données dynamique et les données statics
		DataFrame lSchemaSatic = lSqlContext.read().load(BundelUtils.get("static.path"));
		DataFrame lFinalJoin = lSchemaDBrute
				.join(lSchemaSatic, lSchemaDBrute.col("fStationId").equalTo(lSchemaSatic.col("sStationId")),
						"rightouter")

				// order columns
				.select(lSchemaDBrute.col("fStationId"), lSchemaDBrute.col("fSystemDate"),
						lSchemaDBrute.col("fRealDate"), lSchemaDBrute.col("fRoundedSystemDate"),
						lSchemaDBrute.col("fLaggedRoundedSystemDate"), lSchemaDBrute.col("fBanking"),
						lSchemaDBrute.col("fBonus"), lSchemaDBrute.col("fStatus"), lSchemaDBrute.col("fBikeStands"),
						lSchemaDBrute.col("fAvailableBikeStands"), lSchemaDBrute.col("fAvailableBikes"),
						lSchemaDBrute.col("fMonth"), lSchemaDBrute.col("fDayOfWeek"), lSchemaDBrute.col("fHour"),
						lSchemaDBrute.col("fRoundedMinutes"), lSchemaSatic.col("sLat"), lSchemaSatic.col("sLong"),
						lSchemaSatic.col("sAlt"), lSchemaSatic.col("sPopTot"), lSchemaSatic.col("sArea"),
						lSchemaSatic.col("sPerimeter"), lSchemaSatic.col("sZipcode"))
				// rename column sPopTot to sPopulation
				.withColumnRenamed("sPopTot", "sPopulation");

		// generate parquet
		String lLinkToOutputFile = iParquetPath + sDate.format(lNow);
		Path lPath = Paths.get(lLinkToOutputFile);
		if (Files.notExists(lPath)) {
			new File(lLinkToOutputFile).mkdir();
		}
		lFinalJoin.write().mode(SaveMode.Append).save(lLinkToOutputFile);

		// TODO insert to impala Database
		// finalJoin.registerTempTable("stations");
	}

	// Test
	public void main(String[] args) {
		String lJsonFile;
		SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
		
		// get les données brutes de JcDecaux
		lJsonFile = DataManipulation.getDataBrute(BundelUtils.get("url.stations"), BundelUtils.get("bruteData.path"));
		
		// to parquet
		DataManipulation.getParquets(lJsonFile, BundelUtils.get("data.frame.path"));
	}
}
