package mes.app.traffic.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDate;

@Getter
@Builder
public class TrafficDailyResponse {

    private String service;
    private LocalDate date;
    private Long requestCount;
    private Double totalMb;
    private Double totalGb;
    private Long totalBytes;
    private Double trafficRatio;

}
