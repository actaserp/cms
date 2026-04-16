package mes.app.Scheduler.SchedulerService;

import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.traffic.TrafficService;
import mes.app.traffic.dto.TrafficUploadResponse;
import mes.app.traffic.util.NginxLogParser;
import mes.domain.entity.TrafficDaily;
import mes.domain.repository.TrafficDailyRepository;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;

import static mes.app.traffic.util.TrafficUtil.toGb;
import static mes.app.traffic.util.TrafficUtil.toMb;

@Service
@Slf4j
@RequiredArgsConstructor
public class NginxTrafficService {

    private final TrafficService trafficService;

    private static final String LOG_ROOT = "/var/log/nginx";

    //todo: service가 많아지면 배치저장이 유리
    public void collectYesterdayTraffic() {
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String dateSuffix = yesterday.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        log.info("[트래픽 집계] 시작 - 대상날짜: {}", yesterday);

        File root = new File(LOG_ROOT);
        File[] serviceDirs = root.listFiles(File::isDirectory);

        if (serviceDirs == null || serviceDirs.length == 0) {
            log.warn("[트래픽 집계] 하위 폴더 없음: {}", LOG_ROOT);
            return;
        }

        int successCount = 0;
        int failCount = 0;

        for(File serviceDir : serviceDirs){
            String service = serviceDir.getName();
            File logFile = resolveLogFile(serviceDir, dateSuffix);

            if(logFile == null) {
                log.warn("[트래픽 집계] 파일 없음 - 서비스: {}, 패턴: access.log-{}", service, dateSuffix);
                failCount++;
                continue;
            }

            try{
                log.info("[트래픽 집계] 파싱 시작 - 서비스: {}, 파일: {}", service, logFile.getName());

                NginxLogParser.TrafficResult result = parseLogFile(logFile);

                trafficService.saveTraffic(service, result, yesterday);

                log.info("[트래픽 집계] 완료 - 서비스: {}, 요청수: {}, 트래픽: {} bytes",
                        service, result.requestCount, result.totalBytes);
                successCount++;

            }catch(IOException e){
                log.error("[트래픽 집계] 파싱 실패 - 서비스: {}, 파일: {}, 오류: {}",
                        service, logFile.getName(), e.getMessage(), e);
                failCount++;

            }
        }
        log.info("[트래픽 집계] 종료 - 성공: {}, 실패: {}", successCount, failCount);
    }

    /**
     * .gz 우선 탐색, 없으면 일반 파일 fallback, 둘 다 없으면 null 반환
     */
    private File resolveLogFile(File serviceDir, String dateSuffix){
        File gzFile = new File(serviceDir, "access.log-" + dateSuffix + ".gz");
        if(gzFile.exists()) return gzFile;

        File plainFile = new File(serviceDir, "access.log-" + dateSuffix);
        if (plainFile.exists()) return plainFile;

        return null;
    }

    /**
     * gz 여부에 따라 적절한 InputStream으로 파싱
     */
    private NginxLogParser.TrafficResult parseLogFile(File logFile) throws IOException {
        try (InputStream fis = new FileInputStream(logFile);
             InputStream is = logFile.getName().endsWith(".gz")
                     ? new GZIPInputStream(fis)
                     : fis) {
            return NginxLogParser.parseStream(is);
        }
    }


}
