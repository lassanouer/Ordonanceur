package com.ov.project.dev.crawler;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.ov.SparkManager;
import com.ov.VelibKey;
import com.ov.VelibProvider;
import com.ov.project.commons.SingletonWrappers;
import com.ov.project.mapper.PredictsByStationDTO;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.Constants;

import scala.Tuple2;

public class ClientOVSocket {

	private int lValue = 30;
	private boolean mIsRunning = true;
	private static boolean mIsPredic = true;

	public List<Tuple2<String, PredictsByStationDTO>> mtempMap = new ArrayList<Tuple2<String, PredictsByStationDTO>>();

	/**
	 * 
	 * Thread d' appel aux prédiction
	 */
	public void open() {
		Thread lThread = new Thread(new Runnable() {
			public void run() {
				while (mIsRunning == true) {

					Map<VelibKey, Integer> lMap = new HashMap<VelibKey, Integer>();
					try {
						SparkManager.getInstance().init(BundelUtils.get("hadoop.home"));
						VelibProvider lProvider = new VelibProvider(BundelUtils.get("license.path"));

						// predict throw l'exception de cast de VectorAssembler
						// a OneHotEncoder
						lMap = lProvider.predict(BundelUtils.get("data.frame.path"), BundelUtils.get("static.path"),
								BundelUtils.get("model.path"), BundelUtils.get("hadoop.home"), Calendar.MINUTE, lValue);

						/*
						 * Cette partie sera simulé pour faire l'appel au client
						 */

						// Map => <VelibStation, Prediction>
						// VelibClient client = new VelibClient(Constants.sHost,
						// Constants.sPort, lMap);

						/*
						 * 
						 */
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
			}
		});

		lThread.start();
	}

	/**
	 * stop thread
	 */
	public void close() {
		mIsRunning = false;
	}

	/**
	 * pour simuler la prediction j'ai tenté de déserializer les hashmap
	 * sauvegarder dans le model
	 * 
	 * @param iInput
	 * @return
	 * @throws IOException
	 */
	public static Map<? extends VelibKey, ? extends Integer> LoadHashMap(String iInput) throws IOException {
		Map<VelibKey, Integer> lMap = null;
		FileInputStream lFileStream = null;
		ObjectInputStream lInputStream = null;

		try {
			lFileStream = new FileInputStream(iInput);
			lInputStream = new ObjectInputStream(lFileStream);
			lMap = (HashMap) lInputStream.readObject();
		} catch (IOException var9) {
			var9.printStackTrace();
		} catch (ClassNotFoundException var10) {
			System.out.println("Class not found");
			var10.printStackTrace();
		} finally {
			if (lInputStream != null) {
				lInputStream.close();
			}

			if (lFileStream != null) {
				lFileStream.close();
			}

		}

		return lMap;
	}

	/*
	 * 
	 * Create manual Map Predict() output
	 */
	public static HashMap<VelibKey, Integer> predict() {
		HashMap<VelibKey, Integer> lresultMap = new HashMap<>();
		Random lRandomPredict = new Random();
		String lContrat = "Paris";
		long lTimeStamp = new Date().getTime();
		String lPathIdStation = BundelUtils.get("stations.id.path");

		try (BufferedReader lBuffr = new BufferedReader(new FileReader(lPathIdStation))) {
			String lStationId;
			while ((lStationId = lBuffr.readLine()) != null) {
				Integer lPredict = lRandomPredict.nextInt(10) + 1;
				VelibKey lTempKey = new VelibKey(lStationId, lContrat, lTimeStamp);
				lresultMap.put(lTempKey, lPredict);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return lresultMap;
	}

	/*
	 * 
	 * Chrone de prediction chaque 5 minutes On doit avoire 10 prédictions dans
	 * 50 minutes dont 1 pred temps réele 5 pred 30 minute 4 pred model
	 * journaliére
	 */

	/**
	 * chaque 50 minutes une prédiction de model Hourly
	 * 
	 * @return
	 */
	public String getStationId(VelibKey iVelibKey) {
		return iVelibKey.getStationId();
	}

	public PredictsByStationDTO updatePredictions(VelibKey iVelibKey, Integer iPrediction) {
		Date lNow = new Date();
		PredictsByStationDTO lPredictsByStationDTO = new PredictsByStationDTO();
		lPredictsByStationDTO.setmIdStation(iVelibKey.getStationId());
		lPredictsByStationDTO.addmPredicts("predictionTime=" + lNow.getTime() + ";predictionValue=" + iPrediction);
		return lPredictsByStationDTO;
	}

	public PredictsByStationDTO concatPredictions(PredictsByStationDTO iPred_1, PredictsByStationDTO iPred_2) {
		iPred_1.getmPredicts().addAll(iPred_2.getmPredicts());
		return iPred_1;
	}

	public void listPredict() {
		HashMap<VelibKey, Integer> lHashTemp = predict();
		lHashTemp.forEach((k, v) -> mtempMap
				.add(new Tuple2<String, PredictsByStationDTO>(getStationId(k), updatePredictions(k, v))));
		JavaSparkContext lSparkCtx = SingletonWrappers.sparkContextGetInstance();
		JavaPairRDD<String, PredictsByStationDTO> lMapPred = lSparkCtx.parallelizePairs(mtempMap);
		lMapPred.reduceByKey((a, b) -> concatPredictions(a, b));
	}


	public Thread GoPredThreadHourly() {
		Date lNow = new Date();
		return new Thread(new Runnable() {
			public void run() {
				while (mIsPredic) {
					listPredict();
					try {
						Thread.sleep(Constants.sDixMinutes * 5);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		});
	}

	/**
	 * chaque 5 minutes une préduction de model journaliére 4 fois puis on flipe
	 * une prédiction
	 * 
	 * @return
	 */
	private Thread GoPredThreadDaily() {
		return new Thread(new Runnable() {
			public void run() {
				while (mIsPredic) {
					listPredict();
					try {
						Thread.sleep(Constants.sCinqMinutes);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
	}

	/**
	 * chaque 5 minutes une préduction de model 30 minutes
	 * 
	 * @return
	 */
	private Thread GoPredThread30() {
		return new Thread(new Runnable() {
			public void run() {
				while (mIsPredic) {
					listPredict();
					try {
						Thread.sleep(Constants.sCinqMinutes);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
		});
	}

	/**
	 * stop thread
	 */
	public void endPredict() {
		mIsPredic = false;
	}

}
