package mes.app.Scheduler;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.*;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDate;
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
    private final CmsEbFileGenerateService    cmsEbFileGenerateService;
    private final CmsEb22ReceiveService       cmsEb22ReceiveService;
    private final CmsEc21FileGenerateService  cmsEc21FileGenerateService;
    private final CmsEc22ReceiveService       cmsEc22ReceiveService;

    /** 매일 00:30 실행, 말일에만 다음달 청구 생성 */
    @Scheduled(cron = "0 30 0 * * *", zone = "Asia/Seoul")
    public void runCmsBillingAutoGenerate() {
        // 오늘이 말일인지 체크
        LocalDate today = LocalDate.now();
        if (!today.equals(today.with(TemporalAdjusters.lastDayOfMonth()))) return;

        schedulerExecutor.execute(() -> safeRun(cmsBillingAutoGenerateService::run, "CMS 청구 자동생성"));
    }

    /** D-1 13:00 — PENDING 청구 EB21 생성 + SFTP 전송 */
    @Scheduled(cron = "0 0 13 * * *", zone = "Asia/Seoul")
    public void runCmsEbFileGenerate() {
        schedulerExecutor.execute(() -> safeRun(cmsEbFileGenerateService::run, "CMS EB21 생성+전송"));
    }

    /** D+1 04:00 — EB22 결과파일 수신 → billing SUCCESS/FAIL 처리 */
    @Scheduled(cron = "0 0 4 * * *", zone = "Asia/Seoul")
    public void runCmsEb22Receive() {
        schedulerExecutor.execute(() -> safeRun(cmsEb22ReceiveService::run, "CMS EB22 결과수신"));
    }

    /** D 11:00 — PENDING 당일청구 EC21 생성 + SFTP 전송 (마감 D 12:00) */
    @Scheduled(cron = "0 0 11 * * *", zone = "Asia/Seoul")
    public void runCmsEc21FileGenerate() {
        schedulerExecutor.execute(() -> safeRun(cmsEc21FileGenerateService::run, "CMS EC21 생성+전송"));
    }

    /** D 22:00 — EC22 결과파일 수신 → billing SUCCESS/FAIL 처리 (수신 가능 D 23:00) */
    @Scheduled(cron = "0 0 22 * * *", zone = "Asia/Seoul")
    public void runCmsEc22Receive() {
        schedulerExecutor.execute(() -> safeRun(cmsEc22ReceiveService::run, "CMS EC22 결과수신"));
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
