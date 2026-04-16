package mes.app.filter;


import mes.app.util.RedisService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.*;
import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;


@Component
public class ControllerExecutionTimeAspect implements Filter {

    Logger log = LoggerFactory.getLogger("API_EXEC_TIME_LOGGER");

    @Autowired
    RedisService redisService;

    @Override
    public void doFilter(ServletRequest request, ServletResponse response, FilterChain chain)
            throws IOException, ServletException {

        HttpServletRequest req = (HttpServletRequest) request;
        String uri = req.getRequestURI();
        String contextPath = req.getContextPath();

        // 컨텍스트 패스 제거
        if (uri.startsWith(contextPath)) {
            uri = uri.substring(contextPath.length());
        }


        // /api 로 시작하는 요청만 측정
        if (!uri.startsWith("/api") || isExcludedPath(uri)) {
            chain.doFilter(request, response);
            return;
        }

        long start = System.currentTimeMillis();
        String method = req.getMethod();

        String spjangcd = request.getParameter("spjangcd");
        if(spjangcd != null && !spjangcd.isEmpty()){
            String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));

            String redisKey = "MES:" + spjangcd + ":" + today;

            redisService.incrementValue(redisKey);
        }

        try {
            chain.doFilter(request, response);
        } finally {

            long end = System.currentTimeMillis();
            double seconds = (end - start) / 1000.0;

            log.info("[API 실행시간111] {} {} → {}초",
                    method, uri, String.format("%.3f", seconds));

        }
    }

    private boolean isExcludedPath(String uri){
        return uri.startsWith("/api/system/") || uri.startsWith("/api/common")
                || uri.contains("/pages") || uri.contains("/api/monitoring");
    }

}
