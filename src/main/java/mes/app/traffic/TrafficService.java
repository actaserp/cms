package mes.app.traffic;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.NginxTrafficService;
import mes.app.traffic.dto.TrafficDailyResponse;
import mes.app.traffic.dto.TrafficUploadResponse;
import mes.app.traffic.util.NginxLogParser;
import mes.domain.entity.TrafficDaily;
import mes.domain.repository.TrafficDailyRepository;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import javax.transaction.Transactional;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.stream.Collectors;

import static mes.app.traffic.util.TrafficUtil.toGb;
import static mes.app.traffic.util.TrafficUtil.toMb;

@Service
@RequiredArgsConstructor
@Slf4j
public class TrafficService {

    private final TrafficDailyRepository trafficRepo;

    /**
     * 파일 업로드 후 저장
     */
    public TrafficUploadResponse collectFromUploadedFile(MultipartFile file, String service, LocalDate date) {
        log.info("[트래픽 업로드] 서비스: {}, 날짜: {}, 파일: {}", service, date, file.getOriginalFilename());

        NginxLogParser.TrafficResult result = NginxLogParser.parseStream(file);
        saveTraffic(service, result, date);

        log.info("[트래픽 업로드] {} → 요청: {}건 / {}MB ({}GB)",
                service, result.requestCount, toMb(result.totalBytes), toGb(result.totalBytes));

        return TrafficUploadResponse.builder()
                .service(service)
                .date(date)
                .requestCount(result.requestCount)
                .totalMb(toMb(result.totalBytes))
                .totalGb(toGb(result.totalBytes))
                .message("업로드 및 저장 완료")
                .build();
    }

    /**
     * 날짜 범위 조회
     */
    public List<TrafficDailyResponse> getTrafficByDateRange(LocalDate startDate, LocalDate endDate) {
        List<TrafficDaily> list = trafficRepo.findByDateBetween(startDate, endDate);

        // 전체 합산
        long totalAllBytes = list.stream()
                .mapToLong(TrafficDaily::getTotalBytes)
                .sum();

        return list.stream()
                .map(t -> TrafficDailyResponse.builder()
                        .service(t.getService())
                        .date(t.getDate())
                        .requestCount(t.getRequestCount())
                        .totalBytes(t.getTotalBytes())
                        .totalMb(toMb(t.getTotalBytes()))
                        .totalGb(t.getTotalGb())
                        .trafficRatio(totalAllBytes > 0
                                ? Math.round((t.getTotalBytes() * 100.0 / totalAllBytes) * 100) / 100.0
                                : 0.0)
                        .build())
                .collect(Collectors.toList());
    }

    public List<TrafficDailyResponse> getTrafficGroupedByService(LocalDate startDate, LocalDate endDate) {
        List<Object[]> rows = trafficRepo.findGroupedByService(startDate, endDate);

        long totalAllBytes = rows.stream()
                .mapToLong(r -> ((Number) r[1]).longValue())
                .sum();

        return rows.stream()
                .map(r -> {
                    long bytes = ((Number) r[1]).longValue();
                    return TrafficDailyResponse.builder()
                            .service((String) r[0])
                            .totalBytes(bytes)
                            .requestCount(((Number) r[2]).longValue())
                            .totalMb(toMb(bytes))
                            .totalGb(toGb(bytes))
                            .trafficRatio(totalAllBytes > 0
                                    ? Math.round((bytes * 100.0 / totalAllBytes) * 100) / 100.0
                                    : 0.0)
                            .build();
                })
                .collect(Collectors.toList());
    }

    /**
     * DB upsert
     */
    @Transactional
    public void saveTraffic(String service, NginxLogParser.TrafficResult result, LocalDate date) {
        TrafficDaily entity = trafficRepo
                .findByServiceAndDate(service, date)
                .orElseGet(() -> TrafficDaily.builder()
                        .service(service)
                        .date(date)
                        .build()
                );

        entity.setTotalBytes(result.totalBytes);
        entity.setTotalGb(toGb(result.totalBytes));
        entity.setRequestCount(result.requestCount);
        entity.setRecordedAt(LocalDateTime.now());

        trafficRepo.save(entity);
    }
}
