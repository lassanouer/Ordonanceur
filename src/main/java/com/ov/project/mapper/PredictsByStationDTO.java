package com.ov.project.mapper;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import com.ov.VelibKey;

public class PredictsByStationDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String mIdStation;
	private String mStation; // station
	private List<String> mPredicts = new ArrayList<String>();

	public String getmIdStation() {
		return mIdStation;
	}

	public void setmIdStation(String mIdStation) {
		this.mIdStation = mIdStation;
	}

	public String getmStation() {
		return mStation;
	}

	public void setmStation(String mStation) {
		this.mStation = mStation;
	}

	public List<String> getmPredicts() {
		return mPredicts;
	}

	public void setmPredicts(List<String> mPredicts) {
		this.mPredicts = mPredicts;
	}

	public static String getStationId(VelibKey iVelibKey) {
		return iVelibKey.getStationId();
	}

	public void addmPredicts(String mPredicts) {
		this.mPredicts.add(mPredicts);
	}

	@Override
	public String toString() {
		String lresult = mIdStation + "/station/contractName=Paris;";
		for (String lTemp : mPredicts) {
			lresult.concat(lTemp + ";");
		}
		return lresult;
	}

	public static PredictsByStationDTO concatPredictions(PredictsByStationDTO iPred_1, PredictsByStationDTO iPred_2) {
		boolean lmoreThenTenPred = true;
		iPred_1.getmPredicts().addAll(iPred_2.getmPredicts());
		while (lmoreThenTenPred) {
			if (iPred_1.getmPredicts().size() > 10) {
				iPred_1.getmPredicts().remove(0);
			} else {
				lmoreThenTenPred = false;
			}
		}
		return iPred_1;
	}

	public static HashMap<String, PredictsByStationDTO> mergeHashMap(HashMap<String, PredictsByStationDTO> iMap_1,
			HashMap<String, PredictsByStationDTO> iMap_2) {
		Set<Entry<String, PredictsByStationDTO>> entries = iMap_2.entrySet();
		for (Map.Entry<String, PredictsByStationDTO> entry : entries) {
			PredictsByStationDTO secondMapValue = iMap_1.get(entry.getKey());
			if (secondMapValue == null) {
				iMap_1.put(entry.getKey(), entry.getValue());
			} else {
				secondMapValue = concatPredictions(secondMapValue, entry.getValue());
			}
		}
		return iMap_1;
	}

	public static PredictsByStationDTO concatePredictions(VelibKey iVelibKey, Integer iPrediction) {
		Date lNow = new Date();
		PredictsByStationDTO lPredictsByStationDTO = new PredictsByStationDTO();
		lPredictsByStationDTO.setmIdStation(iVelibKey.getStationId());
		lPredictsByStationDTO.addmPredicts("predictionTime=" + lNow.getTime() + ";predictionValue=" + iPrediction);
		return lPredictsByStationDTO;
	}
}
