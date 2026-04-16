package mes.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;


import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;

@Configuration
@EnableAsync
//비동기 전용 쓰레드풀
public class AsyncConfig {

    @Bean(name = "asyncExecutor")
    public ThreadPoolTaskExecutor asyncExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();

        // 1. 기본 실행 쓰레드 수
        executor.setCorePoolSize(15);

        // 2. 최대 쓰레드 수 (부하가 몰릴 때 생성)
        executor.setMaxPoolSize(30);

        // 3. 큐 용량 (Core 쓰레드가 다 차면 여기서 대기)
        executor.setQueueCapacity(250);

        // 4. 이름 접두사
        executor.setThreadNamePrefix("Global-Async-");

        // 5. [중요] 큐와 최대 쓰레드가 모두 찼을 때의 정책 (CallerRunsPolicy)
        // 넘치는 요청은 현재 요청을 보낸 쓰레드(Main 쓰레드)가 직접 처리하게 해서
        // 요청이 아예 유실되는 것을 방지합니다.
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());

        executor.initialize();
        return executor;
    }
}
