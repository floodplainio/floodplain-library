package com.dexels.monitor.rackermon;

public class SLACalculator {

	public static double UPTIME_99dot9999 = 99.9999;
	public static double UPTIME_99dot999 = 99.999;
	public static double UPTIME_99dot995 = 99.995;
	public static double UPTIME_99dot99 = 99.99;
	public static double UPTIME_99dot9 = 99.9;
	public static double UPTIME_99 = 99.0;
	public static double UPTIME_98 = 98.0;
	public static double UPTIME_95 = 95.0;

	public static double getUptime(double minutesDown, double totalMinutes) {
		double uptime = ((totalMinutes - minutesDown) / totalMinutes ) * 100;
		if (uptime == 100) {
			return 100d;
		} else if (uptime > UPTIME_99dot9999) {
			return UPTIME_99dot9999;
		} else if (uptime > UPTIME_99dot999) {
			return UPTIME_99dot999;
		} else if (uptime > UPTIME_99dot995) {
            return UPTIME_99dot995;
		} else if (uptime > UPTIME_99dot99) {
			return UPTIME_99dot99;
		} else if (uptime > UPTIME_99dot9) {
			return UPTIME_99dot9;
		} else if (uptime > UPTIME_99) {
			return UPTIME_99;
		} else if (uptime > UPTIME_98) {
			return UPTIME_99dot9;
		} else if (uptime > UPTIME_95) {
			return UPTIME_95;
		}
		return -1;
	}

}
