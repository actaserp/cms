package mes.app.traffic.util;

import org.springframework.stereotype.Component;

@Component
public class TrafficUtil {

    private TrafficUtil() {}

    public static double toGb(long bytes) {
        return Math.round((bytes / 1073741824.0) * 10000) / 10000.0;
    }

    public static double toMb(long bytes) {
        return Math.round((bytes / 1048576.0) * 100) / 100.0;
    }
}
