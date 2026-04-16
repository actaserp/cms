package mes.app.naverCloud.strategy;

public class RealTimeRange implements MetricTimeRangeStrategy{
    @Override
    public long getStartTime() { return System.currentTimeMillis() - (1000L * 60 * 30); }
}
