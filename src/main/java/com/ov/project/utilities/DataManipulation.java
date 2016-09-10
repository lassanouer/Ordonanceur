package com.ov.project.utilities;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
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

import com.ov.SparkManager;
import com.ov.VelibProvider;
import com.ov.importDataSources.ImportAPIfile;
import com.ov.project.mapper.StationDTO;

public class DataManipulation {

	// Date format
	public static SimpleDateFormat dateAndTime = new SimpleDateFormat(BundelUtils.get("date.and.time.format"));
	public static SimpleDateFormat date = new SimpleDateFormat(BundelUtils.get("date.format"));

	/**
	 * create parquet Schema of StationDTO
	 * 
	 * @return
	 */
	private static JavaRDD<StationDTO> stationDTOFromJsonConverter(String filename) {
		JavaSparkContext javaSparkContext = SingletonWrappers.sparkContextGetInstance();
		JavaRDD<StationDTO> stations = javaSparkContext.textFile(filename).map(new Function<String, StationDTO>() {
			public StationDTO call(String line) throws Exception {

				// clean text et attributs
				List<String> parts = Arrays.asList(line.replace("[{}[]]+", "").split(",\""));
				for (String part : parts) {
					part = part.replace("\"", "");
				}

				// retour l'objet stationDTO
				return jsonStationToObject(parts);

				// TODO
				// ****************TO VERIF******************
				// ObjectMapper mapper = new ObjectMapper();
				// return mapper.readValue(line, StationDTO.class);
				// ****************TO VERIF******************

			}
		});
		return stations;
	}

	/**
	 * Convertir ligne json to StationDTO object
	 * 
	 * @param iLigneJson
	 * @return
	 */
	public static StationDTO jsonStationToObject(List<String> iLigneJson) {
		Date now = new Date();
		StationDTO station = new StationDTO();
		station.setfStationId(
				iLigneJson.get(0).substring(iLigneJson.get(0).indexOf(":") + 1, iLigneJson.get(0).length()));
		station.setfBanking(Boolean
				.valueOf(iLigneJson.get(5).substring(iLigneJson.get(5).indexOf(":") + 1, iLigneJson.get(5).length()))
				.booleanValue());
		station.setfBonus(Boolean
				.valueOf(iLigneJson.get(6).substring(iLigneJson.get(6).indexOf(":") + 1, iLigneJson.get(6).length()))
				.booleanValue());
		station.setfStatus(iLigneJson.get(7).substring(iLigneJson.get(7).indexOf(":") + 1, iLigneJson.get(7).length()));
		station.setfBikeStands(Float
				.valueOf(iLigneJson.get(9).substring(iLigneJson.get(9).indexOf(":") + 1, iLigneJson.get(9).length()))
				.floatValue());
		station.setfAvailableBikeStands(Float
				.valueOf(iLigneJson.get(10).substring(iLigneJson.get(10).indexOf(":") + 1, iLigneJson.get(10).length()))
				.floatValue());
		station.setfAvailableBikes(Float
				.valueOf(iLigneJson.get(11).substring(iLigneJson.get(11).indexOf(":") + 1, iLigneJson.get(11).length()))
				.floatValue());
		station.setfMonth(String.valueOf(Calendar.getInstance().get(Calendar.MONTH) + 1));
		station.setfDayOfWeek(String.valueOf(Calendar.getInstance().get(Calendar.DAY_OF_WEEK)));
		station.setfHour(String.valueOf(Calendar.getInstance().get(Calendar.HOUR_OF_DAY)));
		station.setfRoundedMinutes(String.valueOf(Calendar.getInstance().get(Calendar.MINUTE)));
		station.setfSystemDate(now.getTime());
		station.setfRealDate(now.getTime());
		station.setfRoundedSystemDate(now.getTime());
		station.setfLaggedRoundedSystemDate(now.getTime());
		return station;
	}

	/**
	 * reception de Json a partir de Url de JcDecaux
	 * 
	 * @param iUrl (BundelUtils.get("url.stations"))
	 * @param iPath (BundelUtils.get("bruteData.path"))
	 * @return
	 */
	public static String getDataBrute(String iUrl, String iPath) {
		Date now = new Date();
		String lFile = BundelUtils.get("suffix.for.data.file") + dateAndTime.format(now) + ".txt";
		Path path = Paths.get(iPath + date.format(now));

		// cree le fichier qui contien les données de la date actuelle
		if (Files.notExists(path))
			new File(iPath + date.format(now)).mkdir();

		try {
			ImportAPIfile.storeJSONFileToTxt(iUrl, iPath + date.format(now), lFile);
		} catch (Exception e) {
			e.printStackTrace();
		}

		// besoin de retourner le file path pour plus simple que l'identifier
		// par date dans la prochaine etape
		return iPath + date.format(now) + "/" + lFile;
	}

	/**
	 * parser un fichier json a parquets
	 * 
	 * @param iJsonPath (BundelUtils.get("bruteData.path"))
	 * @param iParquetPath (BundelUtils.get("data.frame.path"))
	 */
	public static void getParquets(String iJsonPath, String iParquetPath) {
		Date now = new Date();
		// config
		JavaSparkContext sparkCtx = SingletonWrappers.sparkContextGetInstance();
		SQLContext sqlContext = new org.apache.spark.sql.SQLContext(sparkCtx);

		// get data from Json
		JavaRDD<StationDTO> stations = stationDTOFromJsonConverter(iJsonPath);
		DataFrame schemaStations = sqlContext.createDataFrame(stations, StationDTO.class);

		// join les données dynamique et les données statics liée a la
		// géolocalisation
		DataFrame schemaSatic = sqlContext.read().load(BundelUtils.get("static.path"));
		DataFrame finalJoin = schemaStations.join(schemaSatic);

		// generate parquet
		finalJoin.write().save(iParquetPath + dateAndTime.format(now));

		// TODO insert to impala Database
		//finalJoin.registerTempTable("stations");
	}

	// Test
	public static void main(String[] args) {
		String jsonFile;
		SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
		VelibProvider lProvider = new VelibProvider(BundelUtils.get("license.path"));
		// get les données brutes de JcDecaux
		jsonFile = DataManipulation.getDataBrute(BundelUtils.get("url.stations"), BundelUtils.get("bruteData.path"));
		// to parquet
		DataManipulation.getParquets(jsonFile, BundelUtils.get("data.frame.path"));
	}
}
