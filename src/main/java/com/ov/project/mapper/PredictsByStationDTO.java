package com.ov.project.mapper;

import java.io.Serializable;
import java.util.List;

public class PredictsByStationDTO implements Serializable {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String mIdStation;
	private String mStation; // station
	private List<String> mPredicts;

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

	public void addmPredicts(String mPredicts) {
		this.mPredicts.add(mPredicts);
		if (this.mPredicts.size() > 10) {
			this.mPredicts.remove(0);
		}
	}

	@Override
	public String toString() {
		String lresult = mIdStation + "/station/contractName=Paris;";
		for (String lTemp : mPredicts) {
			lresult.concat(lTemp + ";");
		}
		return lresult;
	}

}
