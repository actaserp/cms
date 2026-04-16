package mes.app.analysis;

import lombok.RequiredArgsConstructor;
import mes.app.analysis.service.MesAnalysisService;
import mes.app.analysis.service.MesGptSqlQueryService;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;
@RestController
@RequestMapping("/api/mes")
@RequiredArgsConstructor
public class MesAnalysisController {

    private final MesAnalysisService mesAnalysisService;
    private final MesGptSqlQueryService mesGptSqlQueryService;

    protected Log log =  LogFactory.getLog(this.getClass());

    // 1. MES 분석용 데이터 제공
    @GetMapping("/analysis-data")
    public ResponseEntity<?> getMesDataForAnalysis() {
        log.info("insalist Exception =====>getMesDataForAnalysis");
        List<Map<String, Object>> data = mesAnalysisService.getRecentProcessDefectStats();
        return ResponseEntity.ok(Map.of("success", true, "data", data));
    }

    // 2. GPT 분석 요청 처리
    @PostMapping(value = "/gpt-analyze", consumes = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<?> analyzeWithGpt(@RequestBody Map<String, Object> request) {
        String prompt = (String) request.get("prompt");
        List<Map<String, Object>> data = (List<Map<String, Object>>) request.get("data");

        try {
//            String result = mesAnalysisService.analyzeWithGpt(prompt, data);
            Map<String, Object> result = mesAnalysisService.analyzeWithGptStructured(prompt, data);

            // 자연어 응답만 포함된 경우: message는 문자열이고 tableData는 비어 있음
            Object messageObj = result.get("message");
            Object tableDataObj = result.get("tableData");

            boolean isNaturalResponse = (
                    messageObj instanceof String &&
                            (tableDataObj == null || (tableDataObj instanceof List && ((List<?>) tableDataObj).isEmpty()))
            );

            if (isNaturalResponse) {
                return ResponseEntity.ok(Map.of("answer", messageObj));
            }

            // 그 외는 일반 SQL 응답
            return ResponseEntity.ok(result);

        } catch (MesGptSqlQueryService.GptNaturalResponseException e) {
            // 완전히 자연어만 던졌을 때
            return ResponseEntity.ok(Map.of("answer", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("error", "분석 중 오류 발생: " + e.getMessage()));
        }
    }
}
