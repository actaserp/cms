package mes.app.cms.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Instant;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 금결원 영업일/휴일 조회 서비스
 * - 월별 캐시 (TTL 24시간)
 * - 토큰은 tb_xa012_cms에 등록된 첫 번째 사업장으로 발급 (공통 데이터라 기관 무관)
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class CmsHolidayService {

    private static final DateTimeFormatter FMT = DateTimeFormatter.ofPattern("yyyyMMdd");

    private final SqlRunner       sqlRunner;
    private final CmsTokenService cmsTokenService;
    private final ObjectMapper    objectMapper = new ObjectMapper();
    private final HttpClient      httpClient   = HttpClient.newHttpClient();

    @Value("${cms.api-base-url}")
    private String apiBaseUrl;

    // ── public API ──────────────────────────────────────────────────────────

    /**
     * 특정 날짜가 휴일인지 여부
     * @param date yyyyMMdd
     */
    public boolean isHoliday(String date) {
        String ym = date.substring(0, 6);
        Map<String, Boolean> monthData = getMonthData(ym);
        return Boolean.TRUE.equals(monthData.get(date));
    }

    /**
     * 특정 날짜 이후 첫 번째 영업일 반환 (당일 포함)
     * @param date yyyyMMdd
     * @return yyyyMMdd
     */
    public String getNextBusinessDay(String date) {
        LocalDate d = LocalDate.parse(date, FMT);
        for (int i = 0; i < 30; i++) {
            String candidate = d.format(FMT);
            if (!isHoliday(candidate)) return candidate;
            d = d.plusDays(1);
        }
        log.error("[CmsHoliday] 30일 내 영업일 없음 - date={}", date);
        return date;
    }

    /**
     * 특정 날짜의 다음 영업일 반환 (당일 제외)
     * @param date yyyyMMdd
     * @return yyyyMMdd
     */
    public String getNextBusinessDayAfter(String date) {
        LocalDate d = LocalDate.parse(date, FMT).plusDays(1);
        return getNextBusinessDay(d.format(FMT));
    }

    /**
     * 특정 날짜 이전 첫 번째 영업일 반환 (당일 포함)
     * 말일이 휴일일 때 스케줄러 트리거 날짜 계산에 사용
     * @param date yyyyMMdd
     * @return yyyyMMdd
     */
    public String getPrevBusinessDay(String date) {
        LocalDate d = LocalDate.parse(date, FMT);
        for (int i = 0; i < 30; i++) {
            String candidate = d.format(FMT);
            if (!isHoliday(candidate)) return candidate;
            d = d.minusDays(1);
        }
        log.error("[CmsHoliday] 30일 내 이전 영업일 없음 - date={}", date);
        return date;
    }

    /**
     * 월별 휴일 목록 반환 (프론트 캘린더용)
     * @param ym yyyyMM
     * @return 휴일 날짜 목록 (yyyyMMdd)
     */
    public List<String> getHolidayList(String ym) {
        Map<String, Boolean> monthData = getMonthData(ym);
        List<String> holidays = new ArrayList<>();
        for (Map.Entry<String, Boolean> e : monthData.entrySet()) {
            if (Boolean.TRUE.equals(e.getValue())) holidays.add(e.getKey());
        }
        Collections.sort(holidays);
        return holidays;
    }

    // ── 내부 ────────────────────────────────────────────────────────────────

    private Map<String, Boolean> getMonthData(String ym) {
        // 1. DB 먼저 확인
        List<Map<String, Object>> rows = sqlRunner.getRows(/* skip_tenant_check */
                "SELECT holiday_date, is_holiday FROM cms_holiday WHERE ym = :ym",
                new MapSqlParameterSource("ym", ym));

        if (!rows.isEmpty()) {
            Map<String, Boolean> result = new LinkedHashMap<>();
            for (Map<String, Object> row : rows) {
                result.put(str(row.get("holiday_date")), (Boolean) row.get("is_holiday"));
            }
            return result;
        }

        // 2. DB 없으면 API 호출
        try {
            Map<String, Boolean> data = fetchFromApi(ym);
            // DB 저장
            for (Map.Entry<String, Boolean> e : data.entrySet()) {
                sqlRunner.execute(/* skip_tenant_check */
                        """
                        INSERT INTO cms_holiday (ym, holiday_date, is_holiday)
                        VALUES (:ym, :date, :isHoliday)
                        ON CONFLICT (ym, holiday_date) DO NOTHING
                        """,
                        new MapSqlParameterSource("ym", ym)
                                .addValue("date", e.getKey())
                                .addValue("isHoliday", e.getValue()));
            }
            return data;
        } catch (Exception e) {
            log.error("[CmsHoliday] API 조회 실패 ym={}: {}", ym, e.getMessage());
            return Collections.emptyMap();
        }
    }

    private String str(Object v) { return v != null ? v.toString() : ""; }

    private Map<String, Boolean> fetchFromApi(String ym) throws Exception {
        LocalDate first = LocalDate.parse(ym + "01", FMT);
        LocalDate last  = first.withDayOfMonth(first.lengthOfMonth());

        String spjangcd = "ZZ";

        String token = cmsTokenService.getToken("ZZ");

        String url = apiBaseUrl + "/biz/common/dates"
                + "?is_holiday=true"
                + "&search_start_date=" + first.format(FMT)
                + "&search_end_date="   + last.format(FMT);

        HttpRequest req = HttpRequest.newBuilder()
                .uri(URI.create(url))
                .header("Authorization", "Bearer " + token)
                .GET()
                .build();

        HttpResponse<String> resp = httpClient.send(req, HttpResponse.BodyHandlers.ofString());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("영업일 API 실패: HTTP " + resp.statusCode() + " " + resp.body());
        }

        JsonNode root = objectMapper.readTree(resp.body());
        String respCode = root.path("response_code").asText("");
        if (!"B0000".equals(respCode)) {
            throw new IllegalStateException("영업일 API 오류: " + respCode
                    + " " + root.path("response_message").asText());
        }

        // 해당 월 전체를 영업일(false)로 초기화 후 휴일만 true로 세팅
        Map<String, Boolean> result = new LinkedHashMap<>();
        LocalDate d = first;
        while (!d.isAfter(last)) {
            result.put(d.format(FMT), false);
            d = d.plusDays(1);
        }
        JsonNode data = root.path("data");
        if (data.isArray()) {
            for (JsonNode node : data) {
                String date       = node.path("date").asText();
                boolean isHoliday = node.path("is_holiday").asBoolean(false);
                if (result.containsKey(date)) result.put(date, isHoliday);
            }
        }

        log.info("[CmsHoliday] 조회완료 ym={} spjangcd={} 휴일={}일 token={}", ym, spjangcd,
                result.values().stream().filter(v -> v).count(), token);
        return result;
    }
}