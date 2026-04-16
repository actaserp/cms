package mes.app.Scheduler;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest
class ScheduledTaskRunnerTest {
    @Autowired
    ScheduledTaskRunner taskRunner;

    @Test
    void testSchedulerRunsAsynchronously() throws InterruptedException {
        System.out.println("=== 테스트 시작 ===");

        // 스케줄러 작업 직접 실행
        taskRunner.runScheduledTasks();

        // 출력이 완료될 때까지 기다림 (비동기라 메인 스레드가 먼저 끝나면 로그 못 봄)
        Thread.sleep(5000); // 5초 정도 대기

        System.out.println("=== 테스트 끝 ===");
    }
}