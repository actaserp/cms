package mes.domain.services;

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.Date;

public class DateUtil {
	
	public static Timestamp getNowTimeStamp() {
		Timestamp now = new Timestamp(System.currentTimeMillis());
		return now;
	}	
	
	public static String getNowString() {
		return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date());
	}
	
	public static String getTodayString() {		
		String yyyymmdd = new SimpleDateFormat("yyyy-MM-dd").format(new Date());		
		return yyyymmdd;		
	}
	
	public static String getYear() {
		String yyyy = new SimpleDateFormat("yyyy").format(new Date());
		return yyyy;
	}
	
	public static Timestamp getYesterdayTimestamp() {		
		LocalDateTime localDateTime = LocalDateTime.now();		
		LocalDateTime yesterday = localDateTime.minusDays(1);		
		Timestamp tsYesterday = Timestamp.valueOf(yesterday);		
		return tsYesterday;				
	}
	
	public static String getFirstDayOfMonthByTodayString() {
		String yyyymmdd = LocalDateTime.now().with(TemporalAdjusters.firstDayOfMonth()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return yyyymmdd;
	}
	
	public static String getLastDayOfMonthByTodayString() {
		String yyyymmdd = LocalDateTime.now().with(TemporalAdjusters.lastDayOfMonth()).format(DateTimeFormatter.ofPattern("yyyy-MM-dd"));
		return yyyymmdd;
	}
	
	public static String getHHmmByTodayString() {
		String HHmm = LocalDateTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
		return HHmm;
	}
}
