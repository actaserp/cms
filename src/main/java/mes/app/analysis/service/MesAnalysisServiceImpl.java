package mes.app.analysis.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import mes.app.analysis.util.TablePromptUtil;
import mes.domain.dto.SqlGenerationResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;


@Service
@RequiredArgsConstructor
public class MesAnalysisServiceImpl implements MesAnalysisService {

    @Value("${openai.api.key}")
    private String apiKey;

    private final MesGptSqlQueryService mesGptSqlQueryService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    protected Log log =  LogFactory.getLog(this.getClass());

    @Override
    public List<Map<String, Object>> getRecentProcessDefectStats() {
        return List.of(); // 실제 DB 사용 시 불필요
    }


    public String analyzeWithGpt(String prompt, List<Map<String, Object>> userData) throws InterruptedException {
        // 1. SQL 생성
        SqlGenerationResult result = mesGptSqlQueryService.generateSqlFromPrompt(prompt);

        if (result == null) {
            return "<p>❌ GPT 응답이 없습니다.</p>";
        }
        if (result.isSqlMode()) {
            // GPT가 SQL을 생성한 경우
            List<Map<String, Object>> rows = mesGptSqlQueryService.executeGeneratedSql(result.getContent());

            if (rows == null || rows.isEmpty()) {
                return "⚠️ GPT가 생성한 SQL:\n" + result.getContent() + "\n\n하지만 결과 데이터가 없습니다.";
            }

            StringBuilder sb = new StringBuilder();
//            sb.append("<p>✅ GPT가 생성한 SQL:</p>");
//            sb.append("<pre>").append(result.getContent()).append("</pre>");

            sb.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>");

            // 헤더
            sb.append("<thead><tr>");
            List<String> headers = new ArrayList<>(rows.get(0).keySet());
            for (String header : headers) {
                sb.append("<th>").append(header).append("</th>");
            }
            sb.append("</tr></thead>");

            // 바디
            sb.append("<tbody>");
            for (Map<String, Object> row : rows) {
                sb.append("<tr>");
                for (String header : headers) {
                    Object cell = row.get(header);
                    sb.append("<td>").append(cell != null ? cell.toString() : "").append("</td>");
                }
                sb.append("</tr>");
            }
            sb.append("</tbody>");

            sb.append("</table>");
            return sb.toString();

        } else {
            // 자연어 분석 응답인 경우
            return "<p>🧠 GPT 분석 결과:</p><p>" + result.getContent().replaceAll("\n", "<br/>") + "</p>";

        }
    }
    @Override
    public Map<String, Object> analyzeWithGptStructured(String prompt, List<Map<String, Object>> data) throws InterruptedException {
        // 기존 답변에서 제공한 logic 사용:
        SqlGenerationResult result = mesGptSqlQueryService.generateSqlFromPrompt(prompt);

        if (result == null) {
            return Map.of("message", "<p>❌ GPT 응답이 없습니다.</p>", "tableData", List.of());
        }

        if (result.isSqlMode()) {
            List<Map<String, Object>> rows = mesGptSqlQueryService.executeGeneratedSql(result.getContent());

            StringBuilder sb = new StringBuilder();
            if (rows == null || rows.isEmpty()) {
                sb.append("<p>⚠️ 결과 데이터가 없습니다.</p>");
            } else {
                sb.append("<table border='1' cellpadding='5' cellspacing='0' style='border-collapse: collapse;'>");
                sb.append("<thead><tr>");
                for (String header : rows.get(0).keySet()) {
                    sb.append("<th>").append(header).append("</th>");
                }
                sb.append("</tr></thead><tbody>");
                for (Map<String, Object> row : rows) {
                    sb.append("<tr>");
                    for (String header : row.keySet()) {
                        Object cell = row.get(header);
                        sb.append("<td>").append(cell != null ? cell.toString() : "").append("</td>");
                    }
                    sb.append("</tr>");
                }
                sb.append("</tbody></table>");
            }

            return Map.of(
                    "message", sb.toString(),
                    "tableData", rows != null ? rows : List.of()
            );

        } else {
            return Map.of(
                    "message", "<p>🧠 GPT 분석 결과:</p><p>" + result.getContent().replaceAll("\n", "<br/>") + "</p>",
                    "tableData", List.of()
            );
        }
    }


    private String analyzeResultWithGpt(String prompt, String sql, List<Map<String, Object>> data) {
        try {
            String dataJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(data);
            String fullPrompt = "다음은 사용자의 질문입니다: " + prompt +
                    "\n\nGPT가 생성한 답변:\n" + sql +
                    "\n\n해당 SQL 실행 결과:\n" + dataJson +
                    "\n\n이 결과를 기반으로 요약 및 분석해줘.";

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(apiKey);

            Map<String, Object> body = Map.of(
                    "model", "gpt-4-1106-preview",
                    "messages", List.of(
                            Map.of("role", "user", "content", fullPrompt)
                    )
            );

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);
            RestTemplate restTemplate = new RestTemplate();
            Map<?, ?> response = restTemplate.postForObject("https://api.openai.com/v1/chat/completions", entity, Map.class);
            List<?> choices = (List<?>) response.get("choices");
            Map<?, ?> message = (Map<?, ?>) ((Map<?, ?>) choices.get(0)).get("message");

            return message.get("content").toString();

        } catch (Exception e) {
            return "분석 중 오류 발생: " + e.getMessage();
        }
    }

    @PostConstruct
    public void testKey() {
//        System.out.println("[OpenAI API Key] " + apiKey);
    }
}