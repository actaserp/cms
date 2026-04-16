package mes.app.naverCloud.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class DataQueryRequest {

    private long timeStart;
    private long timeEnd;
    private String productName;
    private String cw_key;
    private String metric;
    private String interval;
    private String aggregation;

    private Map<String, String> dimensions;

    // 기본 생성자
    public DataQueryRequest() {
        this.dimensions = new HashMap<>();
    }

    // 모든 필드를 포함하는 생성자
    public DataQueryRequest(long timeStart, long timeEnd, String productName, String cw_key,
                            String metric, String interval, String aggregation, Map<String, String> dimensions) {
        this.timeStart = timeStart;
        this.timeEnd = timeEnd;
        this.productName = productName;
        this.cw_key = cw_key;
        this.metric = metric;
        this.interval = interval;
        this.aggregation = aggregation;
        this.dimensions = dimensions;
    }

    // 편리한 데이터 세팅을 위한 Helper 메서드 (인스턴스 번호 전용)
    public void setInstanceDimension(String instanceNo) {
        if (this.dimensions == null) {
            this.dimensions = new HashMap<>();
        }
        this.dimensions.put("instanceNo", instanceNo);
    }

}

