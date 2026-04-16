package mes.app.naverCloud.Enum;

import lombok.Getter;

import java.util.Calendar;

@Getter
public enum NcpMetric {
    avg_cpu_used_rto("CPU사용률", "%", "Min5", "AVG"),
    mem_usert("메모리사용률", "%", "Min5", "AVG"),
    avg_snd_bps("아웃바운드 트래픽", "bps", "Day1", "AVG"),
    avg_rcv_bps("인바운드 트래픽", "bps", "Day1", "AVG");


    private final String desc;
    private final String unit;
    private final String defaultInterval;
    private final String defaultAggregation;

    NcpMetric(String desc, String unit, String defaultInterval, String defaultAggregation){
        this.desc = desc;
        this.unit = unit;

        this.defaultInterval = defaultInterval;
        this.defaultAggregation = defaultAggregation;
    }

}
