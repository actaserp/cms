package mes.config;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.stereotype.Component;

@Component
public class RedisTestRunner implements CommandLineRunner {

    private static final Logger logger = (Logger) LoggerFactory.getLogger(RedisTestRunner.class);
    private final RedisTemplate<String, Object> redisTemplate;

    public RedisTestRunner(RedisTemplate<String, Object> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void run(String... args) throws Exception {
        try {
            logger.info("========================================");
            logger.info("Redis 연결 테스트 시작...");

            // 1. 데이터 쓰기
            String testKey = "REDIS_CONNECTION_TEST";
            String testValue = "SUCCESS";
            redisTemplate.opsForValue().set(testKey, testValue);

            // 2. 데이터 읽기
            String result = (String) redisTemplate.opsForValue().get(testKey);

            if ("SUCCESS".equals(result)) {
                logger.info("Redis 연결 상태: [정상]");
                logger.info("테스트 키 읽기 결과: {}", result);
            } else {
                logger.warn("Redis 연결 상태: [비정상 - 값이 일치하지 않음]");
            }
            logger.info("========================================");

        } catch (Exception e) {
            logger.error("========================================");

            logger.error("Redis 연결 테스트 중 에러 발생!");
            logger.error("에러 내용: {}", e.getMessage());
            logger.error("D드라이브의 maven-repo 라이브러리와 도커 Redis 설정을 확인하세요.");
            logger.error("========================================");
        }
    }
}
