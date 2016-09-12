package com.ov.project.mapper;

import java.io.Serializable;
import java.sql.Timestamp;

public class StationDTO implements Serializable {

	private static final long serialVersionUID = 3326531724763418486L;

	private String fStationId;
	private String fMonth;
	private String fDayOfWeek;
	private String fHour;
	private String fRoundedMinutes;
	private String fStatus;
	private Timestamp fSystemDate;
	private Timestamp fRealDate;
	private Timestamp fRoundedSystemDate;
	private Timestamp fLaggedRoundedSystemDate;
	private boolean fBanking;
	private boolean fBonus;
	private float fBikeStands;
	private float fAvailableBikeStands;
	private float fAvailableBikes;

	public Timestamp getfSystemDate() {
		return fSystemDate;
	}

	public void setfSystemDate(Timestamp fSystemDate) {
		this.fSystemDate = fSystemDate;
	}

	public Timestamp getfRealDate() {
		return fRealDate;
	}

	public void setfRealDate(Timestamp fRealDate) {
		this.fRealDate = fRealDate;
	}

	public Timestamp getfRoundedSystemDate() {
		return fRoundedSystemDate;
	}

	public void setfRoundedSystemDate(Timestamp fRoundedSystemDate) {
		this.fRoundedSystemDate = fRoundedSystemDate;
	}

	public Timestamp getfLaggedRoundedSystemDate() {
		return fLaggedRoundedSystemDate;
	}

	public void setfLaggedRoundedSystemDate(Timestamp fLaggedRoundedSystemDate) {
		this.fLaggedRoundedSystemDate = fLaggedRoundedSystemDate;
	}

	public String getfStationId() {
		return fStationId;
	}

	public void setfStationId(String fStationId) {
		this.fStationId = fStationId;
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
