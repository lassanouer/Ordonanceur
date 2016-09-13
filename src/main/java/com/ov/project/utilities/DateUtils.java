package com.ov.project.utilities;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;

/**
 * 
 * @author Anouer.Lassoued
 *
 */
public class DateUtils {

	/**
	 * Cette methode est faite pour arrondir une date à 5 minutes
	 * 
	 * @param iDate
	 * @return
	 */
	public static Date roundFiveMin(Date iDate) {
		Calendar lCalendar = Calendar.getInstance();
		lCalendar.setTime(iDate);

		int lUnroundedMinutes = lCalendar.get(Calendar.MINUTE);
		int lMod = lUnroundedMinutes % 5;
		lCalendar.add(Calendar.MINUTE, lMod < 3 ? -lMod : (5 - lMod));
		return lCalendar.getTime();
	}

	/**
	 * une heure de recule pour la date d'input
	 * 
	 * @param iDate
	 * @return
	 */
	public static Date oneHourBack(Date iDate) {
		Calendar lCalendar = Calendar.getInstance();
		lCalendar.setTime(iDate);
		lCalendar.add(Calendar.HOUR, -1);
		return lCalendar.getTime();
	}
	
	/**
	 * une heure en moins + arrondir date à 5 minute
	 * @return
	 */
	public static Timestamp calculateLaggedRoundedSystemDate() {
		Date lNow = new Date();
		return new Timestamp(DateUtils.oneHourBack(DateUtils.roundFiveMin(lNow)).getTime());
	}
}
