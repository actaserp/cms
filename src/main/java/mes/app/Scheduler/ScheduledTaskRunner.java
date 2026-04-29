package mes.app.Scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAdjusters;
import java.util.concurrent.Executor;

@Component
@Slf4j
@RequiredArgsConstructor
public class ScheduledTaskRunner {

    private final Executor schedulerExecutor;

    private final AccountSyncService          accountSyncService;
    private final ApiUsageService             apiUsageService;
    private final NginxTrafficService         nginxTrafficService;
    private final CmsBillingAutoGenerateService cmsBillingAutoGenerateService;
    private final CmsEb21SendService cmsEb21SendService;
    private final CmsEb22ReceiveService       cmsEb22ReceiveService;
    private final CmsEc21SendService cmsEc21SendService;
    private final CmsEc22ReceiveService       cmsEc22ReceiveService;

    /** 매일 00:30 실행, 말일에만 다음달 청구 생성 */
    @Scheduled(cron = "0 30 0 * * *", zone = "Asia/Seoul")
    public void runCmsBillingAutoGenerate() {
        // 오늘이 말일인지 체크
        LocalDate today = LocalDate.now();
        if (!today.equals(today.with(TemporalAdjusters.lastDayOfMonth()))) return;

        schedulerExecutor.execute(() -> safeRun(cmsBillingAutoGenerateService::run, "CMS 청구 자동생성"));
    }

    /** D-1 15:00 — PENDING 청구 EB21 생성 + SFTP 전송 */
    @Scheduled(cron = "0 0 15 * * *", zone = "Asia/Seoul")
    public void runCmsEbFileGenerate() {
        schedulerExecutor.execute(() -> safeRun(cmsEb21SendService::run, "CMS EB21 생성+전송"));
    }

    /** D+1 04:00 — EB22 결과파일 수신 → billing SUCCESS/FAIL 처리 */
    @Scheduled(cron = "0 0 4 * * *", zone = "Asia/Seoul")
    public void runCmsEb22Receive() {
        schedulerExecutor.execute(() -> safeRun(cmsEb22ReceiveService::run, "CMS EB22 결과수신"));
    }

    /** D 11:00 — PENDING 당일청구 EC21 생성 + SFTP 전송 (마감 D 12:00) */
    @Scheduled(cron = "0 0 11 * * *", zone = "Asia/Seoul")
    public void runCmsEc21FileGenerate() {
        schedulerExecutor.execute(() -> safeRun(cmsEc21SendService::run, "CMS EC21 생성+전송"));
    }

    /** D 22:00 — EC22 결과파일 수신 → billing SUCCESS/FAIL 처리 (수신 가능 D 23:00) */
    @Scheduled(cron = "0 0 22 * * *", zone = "Asia/Seoul")
    public void runCmsEc22Receive() {
        schedulerExecutor.execute(() -> safeRun(cmsEc22ReceiveService::run, "CMS EC22 결과수신"));
    }

    /** EC21 재시도 1차 — D 11:10 */
    @Scheduled(cron = "0 10 11 * * *", zone = "Asia/Seoul")
    public void runCmsEc21Retry1() {
        String targetDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        schedulerExecutor.execute(() -> safeRun(() -> cmsEc21SendService.retry(targetDate), "CMS EC21 재시도1"));
    }

    /** EC21 재시도 2차 — D 11:20 */
    @Scheduled(cron = "0 20 11 * * *", zone = "Asia/Seoul")
    public void runCmsEc21Retry2() {
        String targetDate = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        schedulerExecutor.execute(() -> safeRun(() -> cmsEc21SendService.retry(targetDate), "CMS EC21 재시도2"));
    }

    /** EB21 재시도 1차 — D-1 15:10 */
    @Scheduled(cron = "0 10 15 * * *", zone = "Asia/Seoul")
    public void runCmsEb21Retry1() {
        String targetDate = LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        schedulerExecutor.execute(() -> safeRun(() -> cmsEb21SendService.retry(targetDate), "CMS EB21 재시도1"));
    }

    /** EB21 재시도 2차 — D-1 15:20 */
    @Scheduled(cron = "0 20 15 * * *", zone = "Asia/Seoul")
    public void runCmsEb21Retry2() {
        String targetDate = LocalDate.now().plusDays(1).format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        schedulerExecutor.execute(() -> safeRun(() -> cmsEb21SendService.retry(targetDate), "CMS EB21 재시도2"));
    }

    /** 매일 새벽 3시 — nginx 트래픽 집계 */
    @Scheduled(cron = "0 0 3 * * *")
    public void runDailyTrafficCollect() {
        schedulerExecutor.execute(() -> safeRun(nginxTrafficService::collectYesterdayTraffic, "nginx트래픽집계"));
    }

    private void safeRun(Runnable task, String taskName) {
        try {
            task.run();
        } catch (Exception e) {
            log.error("스케줄러 작업 실패: {} - {}", taskName, e.getMessage(), e);
        }
    }
}
