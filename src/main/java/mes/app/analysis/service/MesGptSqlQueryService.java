package mes.app.analysis.service;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import mes.app.analysis.util.TablePromptUtil;
import mes.domain.dto.SqlGenerationResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class MesGptSqlQueryService {

    @Value("${openai.api.key}")
    private String apiKey;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final JdbcTemplate jdbcTemplate;

    // 기억된 테이블 명세 (GPT가 이해할 수 있도록 system prompt로 활용)
    private final Map<String, List<Map<String, String>>> gptMesTableSpec;

    protected Log log =  LogFactory.getLog(this.getClass());


    public static class SqlGenerationResult02 {
        public String sql;
        public String answer;

    }

    /**
     * 1. 사용자 질문을 기반으로 GPT가 SQL 쿼리 또는 자연어 응답 생성
     */
    public SqlGenerationResult generateSqlFromPrompt(String prompt) throws InterruptedException {
        try {
            String schemaPrompt = TablePromptUtil.buildSystemPromptFromSpec(gptMesTableSpec);

            String fullPrompt = createPrompt(prompt);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            headers.setBearerAuth(apiKey);

            Map<String, Object> body = Map.of(
                    "model", "gpt-4.1-mini-2025-04-14",
                    "messages", List.of(
                            Map.of("role", "system", "content",  schemaPrompt),
                            Map.of("role", "user", "content", fullPrompt)
                    )
            );

            HttpEntity<Map<String, Object>> entity = new HttpEntity<>(body, headers);
            RestTemplate restTemplate = new RestTemplate();
            Map<?, ?> response = restTemplate.postForObject("https://api.openai.com/v1/chat/completions", entity, Map.class);

            if (response == null || !response.containsKey("choices")) {
                log.warn("GPT 응답 없음");
                return null;
            }

            List<?> choices = (List<?>) response.get("choices");
            Map<?, ?> message = (Map<?, ?>) ((Map<?, ?>) choices.get(0)).get("message");
            String content = (String) message.get("content");
            return extractFromGptResponse(content);
        }catch (HttpClientErrorException.TooManyRequests e) {
            log.warn("Rate limit 초과 - 일정 시간 대기 후 재시도");
            Thread.sleep(5000); // 5초 대기
            return generateSqlFromPrompt(prompt); // 재시도
        }

    }

    // GPT 응답에서 sql 또는 answer 추출
    private SqlGenerationResult extractFromGptResponse(String gptResponse) {
        try {
            String cleaned = gptResponse.trim();

            // 마크다운 제거
            if (cleaned.startsWith("```")) {
                cleaned = cleaned.replaceAll("^```(json|sql)?\\s*", "").replaceAll("```\\s*$", "");
            }

            // JSON 파싱 시도
            try {
                Map<String, Object> map = objectMapper.readValue(cleaned, new TypeReference<>() {});
                if (map.containsKey("sql")) {
                    return new SqlGenerationResult(true, map.get("sql").toString());
                } else if (map.containsKey("answer")) {
                    return new SqlGenerationResult(false, map.get("answer").toString());
                }
            } catch (Exception jsonError) {
                log.warn("정상 JSON 아님, fallback 처리 시도");

                // fallback: SQL인지 자연어인지 판단
                if (cleaned.toLowerCase().contains("select") || cleaned.toLowerCase().contains("from")) {
                    return new SqlGenerationResult(true, cleaned);
                } else {
                    return new SqlGenerationResult(false, cleaned);
                }
            }

            return new SqlGenerationResult(false, "GPT 응답을 해석할 수 없습니다.");

        } catch (Exception e) {
            log.error("GPT 응답 파싱 실패", e);
            return null;
        }
    }



    /**
     * 2. 생성된 SQL을 실행하고 결과 반환
     */
    public List<Map<String, Object>> executeGeneratedSql(String sql) {
        try {
            log.info("[OpenAI sql] =====> " + sql);
            return jdbcTemplate.queryForList(sql);
        } catch (Exception e) {
            log.error("SQL 실행 오류", e);
            return null;
        }
    }

    /**
     * 3. 사용자 질문을 기반으로 전체 흐름 처리
     */
    public String run(String prompt) throws InterruptedException {
        SqlGenerationResult result = generateSqlFromPrompt(prompt);

        if (result == null) {
            throw new RuntimeException("GPT 처리 실패: 응답이 null입니다.");
        } else if (result.isSqlMode()) {
            List<Map<String, Object>> rows = executeGeneratedSql(result.getContent());
            return formatRowsAsReadableText(rows);  // 👉 여기에 결과 포맷
        } else if (result.getContent() != null) {
            return result.getContent();  // 자연어 응답
        } else {
            throw new RuntimeException("GPT가 유효한 SQL이나 답변을 반환하지 않았습니다.");
        }
    }
    private String formatRowsAsReadableText(List<Map<String, Object>> rows) {
        if (rows == null || rows.isEmpty()) {
            return "하지만 결과 데이터가 없습니다.";
        }

        Map<String, Object> row = rows.get(0);
        StringBuilder sb = new StringBuilder("분석 결과:\n");

        for (Map.Entry<String, Object> entry : row.entrySet()) {
            sb.append("- ").append(entry.getKey()).append(": ").append(entry.getValue()).append("\n");
        }

        return sb.toString();
    }

    /**

     * 5. GPT에게 보낼 prompt 생성

     */
    public String createPrompt(String param) {
        String schemaPrompt = TablePromptUtil.buildSystemPromptFromSpec(gptMesTableSpec);

        return schemaPrompt + "\n\n" + """
        당신은 MES 시스템 분석 도우미입니다.
        아래는 MES 시스템에서 사용하는 실제 테이블 및 컬럼 명세입니다.
        ✅ 반드시 아래 테이블/컬럼만 사용해서 SQL을 작성하세요.
        ❌ 존재하지 않는 테이블명이나 컬럼명을 사용하면 안 됩니다.
        ✅ SELECT 문만 작성하세요. INSERT/UPDATE/DELETE는 작성하지 마세요.
        - PostgreSQL에서는 테이블명 및 컬럼명을 반드시 쌍따옴표(")로 감싸서 사용해야 합니다.
        - 예시: SELECT * FROM "balju" WHERE "JumunDate" >= '2025-07-01'
        - 존재하지 않는 컬럼명을 사용하지 마세요. 예: TotalPrice (존재하지 않음)
        - 금액 계산은 반드시 명세에 나온 컬럼만 사용해서 계산하세요.     
        PostgreSQL에서 SQL을 생성할 때 다음 조건을 반드시 따른다:                        
        1. ROUND 함수 사용 시, 소수점 자릿수를 지정하려면 double precision 값을 반드시 NUMERIC으로 변환해야 한다.
           PostgreSQL은 round(double precision, integer) 함수 시그니처를 지원하지 않으므로,
           반드시 ::NUMERIC 또는 CAST(... AS NUMERIC) 으로 변환할 것.
           - 예: ROUND((SUM(...) / SUM(...))::NUMERIC, 2)           
        2. 0으로 나누는 오류를 방지하기 위해 분모에는 항상 NULLIF(..., 0)을 사용한다. \s
           예: SUM("A") / NULLIF(SUM("B"), 0)                        
        3. 정수와 실수를 곱하거나 나눌 때에는 실수형으로 계산되도록 100이 아닌 100.0을 사용한다.                        
        4. PostgreSQL은 큰따옴표(")로 감싼 컬럼명을 대소문자 구분하므로, 테이블에 정의된 정확한 컬럼 ID를 사용해야 한다. 예: "DefectQty"                        
        5. 날짜 필터링 시 TO_CHAR 또는 DATE_TRUNC를 사용해 문자열/월별 기준 조건을 정확히 처리해야 한다. \s
           예: TO_CHAR("ProductionDate", 'YYYY-MM') = '2025-06'  
        6. PostgreSQL에서는 ROUND 함수에 소수점 자릿수를 지정하려면 인자로 전달되는 계산식이 double precision일 경우 반드시 명시적으로 NUMERIC으로 형변환해야 한다. \s
           이유는 PostgreSQL이 `round(double precision, integer)` 시그니처를 지원하지 않기 때문이다. \s
           반드시 아래 방식 중 하나를 사용할 것:           
           - ROUND((계산식)::NUMERIC, 2) \s
           - ROUND(CAST(계산식 AS NUMERIC), 2)           
           예시: \s
           ✅ ROUND((SUM("DefectQty") * 100.0 / NULLIF(SUM("GoodQty" + "DefectQty"), 0))::NUMERIC, 2)          
        7. 테이블명이 `tb_`로 시작하는 경우, 컬럼명이 모두 소문자일 수 있으므로 PostgreSQL 쿼리에서는 반드시 큰따옴표(")로 컬럼명을 감싸야 한다. \s
                  PostgreSQL은 소문자 컬럼을 따옴표 없이 참조할 경우 대문자로 인식하여 오류가 발생하므로 주의한다.               
                  예시: SELECT "item_code", "qty" FROM "tb_inventory"  
        8. PostgreSQL에서는 테이블명이 'tb_'로 시작할 경우, 컬럼명이 모두 소문자로 생성되어 있을 수 있다. \s
           이 경우, 반드시 컬럼명을 소문자로 감싼 큰따옴표(")로 정확하게 참조해야 한다. \s
           예: SELECT "misdate" FROM "tb_salesment"
           대문자 또는 따옴표 없는 참조(예: MISDATE)는 PostgreSQL에서 인식되지 않아 오류가 발생하므로 주의한다.    
        9. 쿼리 결과를 사용자에게 보여줄 때, SELECT 절에는 테이블 명세서에 정의된 컬럼의 “설명” 또는 “한글 별칭”을 AS 구문으로 명시해야 한다. \s
           이 별칭은 프론트 화면에서 테이블 헤더로 사용된다.
           예시:
           SELECT "misdate" AS "전표일자", "cltcd" AS "거래처코드" FROM "tb_salesment"
        10. 테이블명이 `tb_`로 시작하는 경우, 실제 컬럼명은 일반적으로 소문자로 생성되어 있다. \s
            이 경우, SELECT 구문에서 컬럼명을 반드시 소문자로 표기하고, 큰따옴표로 감싸야 한다. \s
            그리고 컬럼 설명(한글 이름)은 `AS`로 별칭을 붙여야 한다.
            예:
            ✅ SELECT "misdate" AS "일자", "itemnm" AS "품목"
            ❌ SELECT "MISDATE" AS "일자"  ← 존재하지 않는 컬럼으로 오류 발생
            PostgreSQL에서는 대소문자를 구분하므로 반드시 실제 컬럼명과 일치하는 소문자를 사용해야 하며, \s
            명세서상의 컬럼 ID와 동일하게 소문자로 참조하는 것이 원칙이다.
            이 기준을 철저히 지켜야 쿼리 오류 없이 결과가 정확하게 반환된다.                           
        11. 두 개 이상의 테이블을 조인할 경우, SELECT / WHERE / ORDER BY 절에서 사용하는 모든 컬럼에는 반드시 테이블 별칭을 함께 명시해야 한다. \s
            PostgreSQL에서는 동일한 컬럼명이 여러 테이블에 존재하거나 유사할 경우, 모호한 참조(Ambiguous column) 오류가 발생한다.
            예:
            ✅ ROUND((o."OrderQty" * o."UnitPrice")::NUMERIC, 2)
            ❌ ROUND(("OrderQty" * "UnitPrice")::NUMERIC, 2)  ← 오류 발생 가능성 있음
            특히 JOIN 구문 이후 SELECT나 조건문에서 사용하는 모든 컬럼은 반드시 별칭(`a.`, `b.` 또는 `o.`, `m.` 등)을 붙이도록 한다.
        12. 두 개 이상의 테이블을 조인할 경우, JOIN 조건은 반드시 **서로 다른 테이블 간의 외래키 관계**를 기준으로 지정해야 한다. \s
           - 절대 같은 테이블의 컬럼끼리 조인하지 않는다. \s
           - 예: `ON er."StopCause_id" = sc."id"` (올바른 예시) \s
           - 예: `ON er."Equipment_id" = er."Equipment_id"` (잘못된 예시, 자기 자신과 조인)
           또한, 조인 대상 컬럼은 테이블 명세에 정의된 ID 필드 또는 외래키 필드를 사용해야 한다.                            
        13. PostgreSQL에서 TO_CHAR로 날짜를 변환하려면, 대상 컬럼은 반드시 DATE 또는 TIMESTAMP 타입이어야 한다. \s
            만약 문자열(VARCHAR)로 저장된 날짜라면, TO_DATE()로 먼저 형변환 후 TO_CHAR()를 사용해야 한다.
            ✅ 올바른 예시:
            TO_CHAR(TO_DATE("misdate", 'YYYYMMDD'), 'YYYY-MM') = '2025-06'
            ❌ 잘못된 예시:
                    TO_CHAR("misdate", 'YYYY-MM') ← 문자열이기 때문에 오류 발생     
        📌 수주 관련 금액 계산 규칙 
        - 수주 테이블에서 매출 금액은 반드시 아래 기준을 따른다.
          1. "Price" 컬럼은 단가(UnitPrice) × 수량(Qty)을 이미 포함한 값이다.
             → 따라서 "Price"에 수량을 다시 곱하면 안 된다.
             → 예: SUM("Price") ← 정확
             → 예: SUM("Price" * "Qty") ← ❌ 잘못된 계산 
          2. "UnitPrice"만 있고 "Price"가 없는 경우에는 수량을 곱해서 계산한다.
             → 예: SUM("UnitPrice" * "Qty") ← 정확 
          3. 부가세 포함 금액이 필요할 경우 "Price" + "Vat"를 합산한다.
             → 예: SUM("Price" + "Vat") ← 총 매출액(부가세 포함) 
             - 위 계산 규칙을 지키지 않으면 수치 오류가 발생할 수 있다.
        [월별 비교 조건 추가 규칙] 
        - 사용자가 "전월 대비", "월별 비교", "매출 증감률"과 같은 질의를 할 경우:
          - 불필요하게 WITH 구문과 JOIN을 사용하는 대신,
          - CASE WHEN을 이용하여 한 쿼리 안에서 비교하도록 한다.
          - 예를 들어 6월과 7월 매출 비교는 다음과 같이 작성:
        
            SELECT
              SUM(CASE WHEN TO_CHAR("ShipDate", 'YYYY-MM') = '2025-07' THEN "TotalPrice" + "TotalVat" ELSE 0 END) AS "7월매출금액",
              SUM(CASE WHEN TO_CHAR("ShipDate", 'YYYY-MM') = '2025-06' THEN "TotalPrice" + "TotalVat" ELSE 0 END) AS "6월매출금액",
              ROUND(
                (
                  SUM(CASE WHEN TO_CHAR("ShipDate", 'YYYY-MM') = '2025-07' THEN "TotalPrice" + "TotalVat" ELSE 0 END)
                  - SUM(CASE WHEN TO_CHAR("ShipDate", 'YYYY-MM') = '2025-06' THEN "TotalPrice" + "TotalVat" ELSE 0 END)
                ) * 100.0
                / NULLIF(SUM(CASE WHEN TO_CHAR("ShipDate", 'YYYY-MM') = '2025-06' THEN "TotalPrice" + "TotalVat" ELSE 0 END), 0),
                2
              ) AS "전월대비증감률"
            FROM "shipment_head"
            WHERE "ShipDate" BETWEEN '2025-06-01' AND '2025-07-31'
        
        - 쿼리에서 TO_CHAR(ShipDate, 'YYYY-MM') 조건을 두 번 이상 사용할 경우, 중복 계산을 피하기 위해 WITH 구문을 쓸 수도 있지만, 비교 대상이 2개월 이하인 경우에는 한 줄 CASE WHEN 구문을 우선 사용한다.
                                                   
        ✅ 결과는 JSON 형식으로 다음 중 하나로 반환해야 합니다:
        - SQL이 필요한 경우:
          { "sql": "SELECT ..." }
        - 자연어로 대답할 경우:
          { "answer": "..." }

        사용자 질문은 다음과 같습니다:
        """ + "\n" + param;
    }



    public static class GptNaturalResponseException extends RuntimeException {

        public GptNaturalResponseException(String message) {

            super(message);

        }

    }
}
