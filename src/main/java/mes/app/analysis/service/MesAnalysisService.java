package mes.app.analysis.service;

import java.util.List;
import java.util.Map;

public interface MesAnalysisService {
    List<Map<String, Object>> getRecentProcessDefectStats();
    String analyzeWithGpt(String prompt, List<Map<String, Object>> data) throws InterruptedException;
    Map<String, Object> analyzeWithGptStructured(String prompt, List<Map<String, Object>> data) throws InterruptedException;

}
