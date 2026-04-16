package mes.app.naverCloud.strategy;

import java.util.Calendar;

//한달 정책
public class MonthlyRange implements MetricTimeRangeStrategy{
    @Override
    public long getStartTime() {
        Calendar cal = Calendar.getInstance();
        cal.set(Calendar.DAY_OF_MONTH, 1);
        cal.set(Calendar.HOUR_OF_DAY, 0); cal.set(Calendar.MINUTE, 0);
        cal.set(Calendar.SECOND, 0); cal.set(Calendar.MILLISECOND, 0);
        return cal.getTimeInMillis();
    }
}
