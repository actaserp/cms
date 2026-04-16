package mes.app.Scheduler.SchedulerService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.services.SqlRunner;
import org.springframework.data.redis.core.Cursor;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.core.ScanOptions;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.sql.Date;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Service
@Slf4j
@RequiredArgsConstructor
public class ApiUsageService {

    private final RedisTemplate<String, Object> redisTemplate;
    private final SqlRunner sqlRunner;

    /**
     * TODO: 사업장이 많아지면 delete할때 넣는 Set에 대한 메모리 부담이 커질 수 있음 : 나중에 개선 ㄱㄱ
     * TODO: redisTemplate.opsForValue().get(key) 이것도 레디스 서버 간의 통신을 많이 발생시킴.
     * TODO: 만약 요금제 변경로직이 추가될 경우, 다음달 1일 오전7시부터 적용이 되게 해야할듯함. 바뀐 요금제로 DB에 저장될 수 있음. 해당 전월에 사용한 요금제로 되야 정합성이 맞음. 아니면 결제내역을 보여주던가 요금제 변경에 대한 이력이 필요.
     */
    @Transactional
    public void migrateMonthlyApiUsage() {
        log.info("==== [디버깅] 전월 API 호출 집계 시작 ====");

        String lastMonthPattern = LocalDate.now().minusMonths(1).format(DateTimeFormatter.ofPattern("yyyyMM"));
        // 패턴 예시: MES:*:202602*
        String searchPattern = "MES:*:" + lastMonthPattern + "*";
        log.info("[1] 설정된 스캔 패턴: {}", searchPattern);

        ScanOptions options = ScanOptions.scanOptions().match(searchPattern).count(1000).build();

        List<Map<String, Object>> billPlanList = getBillPlanListBySpjang();
        log.info("[2] DB에서 가져온 사업장 수: {}", billPlanList.size());

        Map<String, Map<String, Object>> spjangInfoMap = billPlanList.stream()
                .collect(Collectors.toMap(
                        m -> String.valueOf(m.get("spjangcd")),
                        m -> m,
                        (oldVal, newVal) -> oldVal
                ));

        List<MapSqlParameterSource> batchList = new ArrayList<>();
        Set<String> keysToDelete = new HashSet<>();

        redisTemplate.execute((RedisCallback<Void>) connection -> {
            int scanCount = 0;
            try (Cursor<byte[]> cursor = connection.scan(options)) {
                while (cursor.hasNext()) {
                    scanCount++;
                    String key = new String(cursor.next());
                    //log.info("[3-스캔중] 발견된 키: {}", key); // 키가 실제로 발견되는지 확인

                    String[] parts = key.split(":");
                    if (parts.length < 3) {
                        //log.warn("[경고] 키 형식이 맞지 않음: {}", key);
                        continue;
                    }

                    String spjangcd = parts[1];
                    String dateStr = parts[2];

                    Object val = redisTemplate.opsForValue().get(key);
                    long count = (val != null) ? Long.parseLong(val.toString()) : 0;

                    Map<String, Object> info = spjangInfoMap.get(spjangcd);
                    String spjangnm = (info != null) ? String.valueOf(info.getOrDefault("spjangnm", "")) : "알수없음";
                    String billPlanName = (info != null) ? String.valueOf(info.getOrDefault("name", "")) : "알수없음";

                    Object priceObj = (info != null) ? info.get("price") : null;
                    BigDecimal price = (priceObj != null) ? new BigDecimal(String.valueOf(priceObj)) : BigDecimal.ZERO;

                    Object limitObj = (info != null) ? info.get("api_call_limit") : null;
                    int limit = (limitObj != null) ? Integer.parseInt(String.valueOf(limitObj)) : 0;

                    Object extra_price_Obj = (info != null) ? info.get("extra_api_unit_price") : null;
                    BigDecimal extra_api_unit_price = (extra_price_Obj != null) ? new BigDecimal(String.valueOf(extra_price_Obj)) : BigDecimal.ZERO;


                    LocalDate rowDate = LocalDate.parse(dateStr, DateTimeFormatter.ofPattern("yyyyMMdd"));
                    batchList.add(new MapSqlParameterSource()
                            .addValue("stat_day", Date.valueOf(rowDate))
                            .addValue("spjangcd", spjangcd)
                            .addValue("spjangnm", spjangnm)
                            .addValue("bill_plan_name", billPlanName)
                            .addValue("price", price)
                            .addValue("api_call_limit", limit)
                            .addValue("extra_api_unit_price", extra_api_unit_price)
                            .addValue("total_count", count)
                    );
                    keysToDelete.add(key);
                }
            } catch (Exception e) {
                log.error("[에러] SCAN 처리 중 예외 발생", e);
            }
            //log.info("[4] 스캔 종료. 총 발견 키 개수: {}", scanCount);
            return null;
        });

        //log.info("[5] 최종 배치 리스트 크기: {}", batchList.size());

        if (batchList.isEmpty()) {
            log.warn("[종료] 이관할 데이터가 없습니다. (Redis에 해당 월 키가 없을 수 있음)");
            return;
        }

        String sql = """
                INSERT INTO api_log_entry (
                    stat_day, spjangcd, spjangnm, bill_plan_name,
                    total_count, price, api_call_limit, extra_api_unit_price
                )
                VALUES (
                    :stat_day, :spjangcd, :spjangnm, :bill_plan_name,
                    :total_count, :price, :api_call_limit, :extra_api_unit_price
                )
                ON CONFLICT (stat_day, spjangcd)
                DO UPDATE SET
                    total_count = EXCLUDED.total_count,
                    spjangnm = EXCLUDED.spjangnm,
                    bill_plan_name = EXCLUDED.bill_plan_name,
                    price = EXCLUDED.price,                       -- 누락 방지
                    api_call_limit = EXCLUDED.api_call_limit,     -- 누락 방지
                    extra_api_unit_price = EXCLUDED.extra_api_unit_price -- 누락 방지
        """;

        SqlParameterSource[] batchArgs = batchList.toArray(new SqlParameterSource[0]);
        int[] result = sqlRunner.batchUpdate(sql, batchArgs);
        log.info("[6] DB 업데이트 성공 행 수: {}", result.length);

        if (!keysToDelete.isEmpty()) {
            redisTemplate.delete(keysToDelete);
            log.info("[7] Redis 키 {}건 삭제 완료. {} 월분 이관 종료", keysToDelete.size(), lastMonthPattern);
        }
    }

    private List<Map<String, Object>> getBillPlanListBySpjang(){
        String sql = """
                select a.spjangcd, a.spjangnm, b.name, b.price, b.api_call_limit, b.extra_api_unit_price
                      from tb_xa012 a
                      left join bill_plans b on a.bill_plans_id = b.id
                      where a.state = 'O'
                """;

                return sqlRunner.getRows(sql, new MapSqlParameterSource());
    }

}
