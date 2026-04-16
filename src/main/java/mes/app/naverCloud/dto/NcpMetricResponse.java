package mes.app.naverCloud.dto;

import lombok.Getter;
import lombok.NoArgsConstructor;

import java.util.List;

@Getter
@NoArgsConstructor
public class NcpMetricResponse {

    private String metric;
    private List<List<Double>> dps;

    public double getAverageValue(){
        if(dps == null || dps.isEmpty()) return 0.0;

        return dps.stream()
                .filter(dp -> dp.size() >= 2)
                .mapToDouble(dp -> dp.get(1))
                .average()
                .orElse(0.0);
    }
}
