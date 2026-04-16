package mes.app.traffic.dto;

import lombok.Builder;
import lombok.Getter;

import java.time.LocalDate;

@Getter
@Builder
public class TrafficUploadResponse {

    private String service;
    private LocalDate date;
    private long requestCount;
    private double totalMb;
    private double totalGb;
    private String message;
}
