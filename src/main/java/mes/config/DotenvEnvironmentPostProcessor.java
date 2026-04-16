package mes.config;

import io.github.cdimascio.dotenv.Dotenv;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.env.EnvironmentPostProcessor;
import org.springframework.core.Ordered;
import org.springframework.core.env.ConfigurableEnvironment;
import org.springframework.core.env.MapPropertySource;

import java.util.HashMap;
import java.util.Map;

public class DotenvEnvironmentPostProcessor
        implements EnvironmentPostProcessor, Ordered {

    @Override
    public int getOrder() {
        return Ordered.HIGHEST_PRECEDENCE;
    }

    @Override
    public void postProcessEnvironment(
            ConfigurableEnvironment environment,
            SpringApplication application) {

        String confPath = resolveConfPath();

        Dotenv dotenv = Dotenv.configure()
                .directory(confPath)
                .ignoreIfMissing()
                .load();

        Dotenv serverEnv = Dotenv.configure()
                .directory(confPath)
                .filename("server.env")
                .ignoreIfMissing()
                .load();

        Map<String, Object> map = new HashMap<>();

        dotenv.entries().forEach(e -> map.put(e.getKey(), e.getValue()));
        serverEnv.entries().forEach(e -> map.put(e.getKey(), e.getValue()));

        // ✅ Spring Environment 우선 주입
        environment.getPropertySources()
                .addFirst(new MapPropertySource("dotenv", map));

        // ✅ 기존 코드 호환용 (윈도우에서 특히 중요)
        map.forEach((k, v) -> {
            if (System.getProperty(k) == null) {
                System.setProperty(k, v.toString());
            }
        });
    }

    private String resolveConfPath() {

        // 1️⃣ JVM 옵션 최우선
        String path = System.getProperty("CONF_PATH");
        if (path != null && !path.isBlank()) return path;

        // 2️⃣ OS 환경변수
        path = System.getenv("CONF_PATH");
        if (path != null && !path.isBlank()) return path;

        // 3️⃣ OS 자동 분기
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("win")) {
            return "C:/Temp/mes21/conf";
        }

        // 4️⃣ 리눅스 기본값
        return "/opt/conf";
    }
}
