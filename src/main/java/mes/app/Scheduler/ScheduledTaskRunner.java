package mes.app.Scheduler;


import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.app.Scheduler.SchedulerService.AccountSyncService;
import mes.app.Scheduler.SchedulerService.ApiUsageService;
import mes.app.Scheduler.SchedulerService.NginxTrafficService;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.concurrent.Executor;

@Component
@Slf4j
@RequiredArgsConstructor
public class ScheduledTaskRunner {

    private final Executor schedulerExecutor;

    private final AccountSyncService accountSyncService;
    private final ApiUsageService apiUsageService;
    private final NginxTrafficService nginxTrafficService;

    //@Scheduled(cron = "0 5 * * * *")
    @Scheduled(cron = "0 0 * * * *") //5분주기
    public void runScheduledTasks() {
        //TODO: 작업 추가할거면 SchedulerThreadPoolConfig 에서 쓰레드풀 조정해주삼 지금 작업하나밖에 없어서 2개로 해놨삼

        //log.info("[스케줄러 시작] 계좌수집 작업 시작 - Thread: {}", Thread.currentThread().getName());

        //schedulerExecutor.execute(() -> safeRun(accountSyncService::run, "계좌수집"));
        //schedulerExecutor.execute(() -> safeRun(apiTimeLogCollectService::run, "API경과시간"));
    }

    @Scheduled(cron = "0 0 3 * * *") // 매일 새벽 3시
    public void runDailyTrafficCollect() {
        schedulerExecutor.execute(() -> safeRun(nginxTrafficService::collectYesterdayTraffic, "nginx트래픽집계"));
    }


    /**
     * todo: 만약 해당 스케줄러 작업이 실패했을 경우에 대한 대비책도 강구해야함.
     */
//    @Scheduled(cron = "0 0 3 1 * *", zone = "Asia/Seoul") /// 매월 1일 새벽 3시
//    //@Scheduled(cron = "0 07 14 * * *", zone = "Asia/Seoul")
//    public void runApiUsageMigration(){
//
//        // 스케줄러가 인식하는 현재 시간 로그 출력
//        String currentTime = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
//        log.info("[스케줄러 감지] 현재 서버 시간: {} | 작업명: api 콜 집계", currentTime);
//
//        schedulerExecutor.execute(() -> safeRun(apiUsageService::migrateMonthlyApiUsage, "api 콜 집계"));
//    }


    private void safeRun(Runnable task, String taskName){
        try{
            task.run();
        }catch (Exception e){
            log.error("스케줄러 작업 실패: {} - {}", taskName, e.getMessage(), e);
        }
    }
}
