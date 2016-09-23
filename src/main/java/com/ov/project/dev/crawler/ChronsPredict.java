package com.ov.project.dev.crawler;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Random;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.ov.VelibKey;
import com.ov.project.mapper.PredictsByStationDTO;
import com.ov.project.utilities.BundelUtils;
import com.ov.project.utilities.Constants;

public class ChronsPredict {

	private static boolean mIsPredic = true;
	private Lock mLock = new ReentrantLock();
	public static HashMap<String, PredictsByStationDTO> mtempMap = new HashMap<String, PredictsByStationDTO>();
	private static HashMap<String, PredictsByStationDTO> mpredictMap = new HashMap<String, PredictsByStationDTO>();

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

	public void listPredict() {
		HashMap<VelibKey, Integer> lHashTemp = predict();
		lHashTemp.forEach((k, v) -> mtempMap.put(PredictsByStationDTO.getStationId(k),
				PredictsByStationDTO.concatePredictions(k, v)));
		setMpredictMap(PredictsByStationDTO.mergeHashMap(getMpredictMap(), mtempMap));
	}

	/**
	 * Hourly prediction qui va prendre la valeur de préduction dépuis JcDecaux
	 * directement celle ci sera déclanché pour les prédiction instantané, a
	 * condition que le dernier client a dépasser une minute
	 * 
	 * @return
	 */
	public Thread GoPredThreadHourly() {
		return new Thread(new Runnable() {
			public void run() {
				while (mIsPredic) {
					mLock.lock();
					try {
						listPredict();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						mLock.unlock();
					}
				}
			}
		});
	}

	/**
	 * chaque 5 minutes une préduction de model journaliére 
	 * 
	 * @return
	 */
	private Thread GoPredThreadDaily() {
		return new Thread(new Runnable() {
			public void run() {
				while (mIsPredic) {
					mLock.lock();
					try {
						listPredict();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						mLock.unlock();
					}
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
					mLock.lock();
					try {
						listPredict();
					} catch (Exception e) {
						e.printStackTrace();
					} finally {
						mLock.unlock();
					}
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

	public HashMap<String, PredictsByStationDTO> getMpredictMap() {
		return mpredictMap;
	}

	public static void setMpredictMap(HashMap<String, PredictsByStationDTO> mpredictMap) {
		ChronsPredict.mpredictMap = mpredictMap;
	}

	public void Start() {
		Thread lthreadDaily = GoPredThreadDaily();
		Thread lthread30 = GoPredThread30();
		lthreadDaily.start();
		lthread30.start();
		// look for Amilioration --- juste to test
	}

}
