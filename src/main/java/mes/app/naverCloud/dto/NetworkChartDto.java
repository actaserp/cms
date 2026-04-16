package mes.app.naverCloud.dto;

import lombok.Getter;
import lombok.Setter;

import java.util.ArrayList;
import java.util.List;

@Getter @Setter
public class NetworkChartDto {

    private List<String> labels = new ArrayList<>();
    private List<Double> inboundData = new ArrayList<>();
    private List<Double> outboundData = new ArrayList<>();

    public static double round(double value){
        return Math.round(value * 100.0) / 100.0;
    }
}
