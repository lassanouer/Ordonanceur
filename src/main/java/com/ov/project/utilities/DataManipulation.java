package com.ov.project.utilities;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;

import com.ov.SparkManager;
import com.ov.importDataSources.ImportAPIfile;
import com.ov.project.mapper.StationDTO;

public class DataManipulation {

	// Date format
	public static SimpleDateFormat sDateAndTime = new SimpleDateFormat(BundelUtils.get("date.and.time.format"));
	public static SimpleDateFormat sDate = new SimpleDateFormat(BundelUtils.get("date.format"));

	/**
	 * create parquet Schema of StationDTO
	 * 
	 * @return
	 */
	private static JavaRDD<StationDTO> stationDTOFromJsonConverter(String iFilename) {
		JavaSparkContext lJavaSparkContext = SingletonWrappers.sparkContextGetInstance();
		JavaRDD<StationDTO> lStations = lJavaSparkContext.textFile(iFilename).map(new Function<String, StationDTO>() {
			public StationDTO call(String line) throws Exception {

				// clean text et attributs
				List<String> lUnits = Arrays.asList(line.replace("[{}[]]+", "").split(",\""));
				for (String lTemp : lUnits) {
					lTemp = lTemp.replace("\"", "");
				}

				// retour l'objet stationDTO
				return jsonStationToObject(lUnits);

				// TODO
				// ****************TO VERIF******************
				// ObjectMapper mapper = new ObjectMapper();
				// return mapper.readValue(line, StationDTO.class);
				// ****************TO VERIF******************

			}
		});
		return lStations;
	}

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
		lStation.setfRoundedSystemDate(new Timestamp(lNow.getTime()));
		lStation.setfLaggedRoundedSystemDate(new Timestamp(lNow.getTime()));
		return lStation;
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

		// join les données dynamique et les données statics liée a la
		// géolocalisation
		DataFrame lSchemaSatic = lSqlContext.read().load(BundelUtils.get("static.path"));
		DataFrame lFinalJoin = lSchemaDBrute
				.join(lSchemaSatic, lSchemaDBrute.col("fStationId").equalTo(lSchemaSatic.col("sStationId")),
						"rightouter")
				.select(lSchemaDBrute.col("fStationId"), lSchemaDBrute.col("fSystemDate"),
						lSchemaDBrute.col("fRealDate"), lSchemaDBrute.col("fRoundedSystemDate"),
						lSchemaDBrute.col("fLaggedRoundedSystemDate"), lSchemaDBrute.col("fBanking"),
						lSchemaDBrute.col("fBonus"), lSchemaDBrute.col("fStatus"), lSchemaDBrute.col("fBikeStands"),
						lSchemaDBrute.col("fAvailableBikeStands"), lSchemaDBrute.col("fAvailableBikes"),
						lSchemaDBrute.col("fMonth"), lSchemaDBrute.col("fDayOfWeek"), lSchemaDBrute.col("fHour"),
						lSchemaDBrute.col("fRoundedMinutes"), lSchemaSatic.col("sLat"), lSchemaSatic.col("sLong"),
						lSchemaSatic.col("sAlt"), lSchemaSatic.col("sPopTot"), lSchemaSatic.col("sArea"),
						lSchemaSatic.col("sPerimeter"), lSchemaSatic.col("sZipcode"))
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
	public static void main(String[] args) {
		String lJsonFile;
		SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
		// get les données brutes de JcDecaux
		lJsonFile = DataManipulation.getDataBrute(BundelUtils.get("url.stations"), BundelUtils.get("bruteData.path"));
		// to parquet
		DataManipulation.getParquets(lJsonFile, BundelUtils.get("data.frame.path"));
	}
}
