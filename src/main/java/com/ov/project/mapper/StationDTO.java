package com.ov.project.mapper;

import java.io.Serializable;

public class StationDTO implements Serializable {

	private static final long serialVersionUID = 3326531724763418486L;

	private String fStationId;
	private String fMonth;
	private String fDayOfWeek;
	private String fHour;
	private String fRoundedMinutes;
	private String fStatus;
	private long fSystemDate;
	private long fRealDate;
	private long fRoundedSystemDate;
	private long fLaggedRoundedSystemDate;
	private boolean fBanking;
	private boolean fBonus;
	private float fBikeStands;
	private float fAvailableBikeStands;
	private float fAvailableBikes;
	// private String sZipcode;
	// private float sLat;
	// private float sLong;
	// private float sAlt;
	// private float sPopulation;
	// private float sArea;
	// private float sPerimeter;

	public String getfStationId() {
		return fStationId;
	}

	public void setfStationId(String fStationId) {
		this.fStationId = fStationId;
	}

	public long getfSystemDate() {
		return fSystemDate;
	}

	public void setfSystemDate(long l) {
		this.fSystemDate = l;
	}

	public long getfRealDate() {
		return fRealDate;
	}

	public void setfRealDate(long fRealDate) {
		this.fRealDate = fRealDate;
	}

	public long getfRoundedSystemDate() {
		return fRoundedSystemDate;
	}

	public void setfRoundedSystemDate(long fRoundedSystemDate) {
		this.fRoundedSystemDate = fRoundedSystemDate;
	}

	public long getfLaggedRoundedSystemDate() {
		return fLaggedRoundedSystemDate;
	}

	public void setfLaggedRoundedSystemDate(long fLaggedRoundedSystemDate) {
		this.fLaggedRoundedSystemDate = fLaggedRoundedSystemDate;
	}

	public boolean isfBanking() {
		return fBanking;
	}

	public void setfBanking(boolean fBanking) {
		this.fBanking = fBanking;
	}

	public boolean isfBonus() {
		return fBonus;
	}

	public void setfBonus(boolean fBonus) {
		this.fBonus = fBonus;
	}

	public String getfStatus() {
		return fStatus;
	}

	public void setfStatus(String fStatus) {
		this.fStatus = fStatus;
	}

	public float getfBikeStands() {
		return fBikeStands;
	}

	public void setfBikeStands(float fBikeStands) {
		this.fBikeStands = fBikeStands;
	}

	public float getfAvailableBikeStands() {
		return fAvailableBikeStands;
	}

	public void setfAvailableBikeStands(float fAvailableBikeStands) {
		this.fAvailableBikeStands = fAvailableBikeStands;
	}

	public float getfAvailableBikes() {
		return fAvailableBikes;
	}

	public void setfAvailableBikes(float fAvailableBikes) {
		this.fAvailableBikes = fAvailableBikes;
	}

	public String getfMonth() {
		return fMonth;
	}

	public void setfMonth(String fMonth) {
		this.fMonth = fMonth;
	}

	public String getfDayOfWeek() {
		return fDayOfWeek;
	}

	public void setfDayOfWeek(String fDayOfWeek) {
		this.fDayOfWeek = fDayOfWeek;
	}

	public String getfHour() {
		return fHour;
	}

	public void setfHour(String fHour) {
		this.fHour = fHour;
	}

	public String getfRoundedMinutes() {
		return fRoundedMinutes;
	}

	public void setfRoundedMinutes(String fRoundedMinutes) {
		this.fRoundedMinutes = fRoundedMinutes;
	}

}
