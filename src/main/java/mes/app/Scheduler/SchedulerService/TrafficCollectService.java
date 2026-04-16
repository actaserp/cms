package mes.app.Scheduler.SchedulerService;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.domain.entity.TrafficDailyUsage;
import mes.domain.repository.TrafficDailyEndpointRepository;
import mes.domain.repository.TrafficDailyUsageRepository;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Service;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.Set;

import static mes.app.traffic.util.TrafficUtil.toGb;
import static mes.app.traffic.util.TrafficUtil.toMb;

@Service
@Slf4j
@RequiredArgsConstructor
public class TrafficCollectService {

    private final RedisTemplate<String, String> redisTemplate;
    private final TrafficDailyUsageRepository usageRepository;
    private final TrafficDailyEndpointRepository endpointRepository;

    //redis에 있는 데이터 rdb로 저장
    public void collectYesterdayTrafficByRedis(){
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String dateSuffix = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        log.info("[트래픽 수집] 시작 - 대상날짜: {}", yesterday);

        Set<String> keys = redisTemplate.keys("*:STATS:*:" + dateSuffix);
        if(keys == null || keys.isEmpty()){
            log.warn("[트래픽 수집] 해당 날짜 키 없음: {}", dateSuffix);
            return;
        }

        for(String key : keys){

            try {
                String[] parts = key.split(":");
                if(parts.length != 4) continue;

                String service = parts[0];
                String company = parts[2];

                Map<Object, Object> data = redisTemplate.opsForHash().entries(key);
                if (data.isEmpty()) continue;

                long totalCount = parseLong(data, "total_count");
                long totalBytes = parseLong(data, "total_bytes");

                TrafficDailyUsage usage = usageRepository
                        .findByServiceAndCompanyAndDate(service, company, yesterday)
                        .map(existing -> {
                            return TrafficDailyUsage.builder()
                                    .id(existing.getId())
                                    .service(service)
                                    .company(company)
                                    .date(yesterday)
                                    .totalCount(totalCount)
                                    .totalBytes(totalBytes)
                                    .totalMb(toMb(totalBytes))
                                    .totalGb(toGb(totalBytes))
                                    .build();
                        })
                        .orElse(TrafficDailyUsage.builder()
                                .service(service)
                                .company(company)
                                .date(yesterday)
                                .totalCount(totalCount)
                                .totalBytes(totalBytes)
                                .totalMb(toMb(totalBytes))
                                .totalGb(toGb(totalBytes))
                                .build());

                        usage = usageRepository.save(usage);

                        log.info("[트래픽 수집] {} / {} → {}건 / {}MB",
                        service, company, totalCount, toMb(totalBytes));

                /*// 2. traffic_daily_endpoint upsert
                Map<String, Map<String, Long>> endpointMap = parseEndpoints(data);

                for (Map.Entry<String, Map<String, Long>> entry : endpointMap.entrySet()) {

                    String endpoint = entry.getKey();
                    Map<String, Long> stat = entry.getValue();
                    Long usageId = usage.getId();

                    TrafficDailyEndpoint ep = endpointRepository
                            .findByServiceAndCompanyAndDateAndEndpoint(service, company, yesterday, endpoint)
                            .map(existing -> TrafficDailyEndpoint.builder()
                                    .id(existing.getId())
                                    .usageId(usageId)
                                    .service(service)
                                    .company(company)
                                    .date(yesterday)
                                    .endpoint(endpoint)
                                    .count(stat.get("count"))
                                    .bytes(stat.get("bytes"))
                                    .elapsed(stat.get("elapsed"))
                                    .build())
                            .orElse(TrafficDailyEndpoint.builder()
                                    .usageId(usageId)
                                    .service(service)
                                    .company(company)
                                    .date(yesterday)
                                    .endpoint(endpoint)
                                    .count(stat.get("count"))
                                    .bytes(stat.get("bytes"))
                                    .elapsed(stat.get("elapsed"))
                                    .build());

                    endpointRepository.save(ep);
                }

                // 3. Redis 키 삭제
                redisTemplate.delete(key);
                log.info("[트래픽 수집] Redis 키 삭제 완료: {}", key);*/

            }catch (Exception e){
                log.error("[트래픽 수집] 키 처리 실패: {}", key, e);
            }
        }

        log.info("[트래픽 수집] 완료");
    }

//    private Map<String, Map<String, Long>> parseEndpoints(Map<Object, Object> data) {
//
//        Map<String, Map<String, Long>> result = new HashMap<>();
//
//        for (Map.Entry<Object, Object> entry : data.entrySet()) {
//
//            String field = entry.getKey().toString();
//            if (!field.contains(":")) continue;
//
//            int lastColon = field.lastIndexOf(":");
//            String endpoint = field.substring(0, lastColon);
//            String statKey  = field.substring(lastColon + 1);
//
//            result.computeIfAbsent(endpoint, k -> new HashMap<>())
//                    .put(statKey, parseLong(entry.getValue().toString()));
//        }
//
//        return result;
//    }

    private long parseLong(Map<Object, Object> data, String key) {
        Object val = data.get(key);
        return val != null ? Long.parseLong(val.toString()) : 0L;
    }

    private long parseLong(String val) {
        try { return Long.parseLong(val); } catch (Exception e) { return 0L; }
    }

}
