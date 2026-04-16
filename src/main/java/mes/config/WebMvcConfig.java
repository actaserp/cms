package mes.config;


import mes.app.interceptor.SpjangSecurityInterceptor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.servlet.config.annotation.CorsRegistry;
import org.springframework.web.servlet.config.annotation.InterceptorRegistry;
import org.springframework.web.servlet.config.annotation.WebMvcConfigurer;


@Configuration
public class WebMvcConfig implements WebMvcConfigurer{

    @Autowired
    private SpjangSecurityInterceptor spjangSecurityInterceptor;

    @Override
    public void addInterceptors(InterceptorRegistry registry) {
        registry.addInterceptor(spjangSecurityInterceptor)
                .addPathPatterns("/api/**")
                .excludePathPatterns(
                        // 1. 인증 관련 (Security permitAll과 일치)
                        "/login", "/logout", "/postLogin", "/intro", "/error", "/alive", "/bill_plan_read", "/biz/save",

                        // 2. 정적 리소스 (이게 빠지면 화면 레이아웃이 깨집니다)
                        "/resource/**", "/img/**", "/images/**", "/js/**", "/css/**",
                        "/assets_mobile/**", "/font/**", "/robots.txt", "/favicon.ico",

                        // 3. 외부 연동 및 예외 API
                        "/useridchk/**", "/user-auth/**", "/popbill/webhook",
                        "/api/transaction/input/**", "/api/das_device", "/authentication/**",

                        // 4. PDA 관련
                        "/pda/**", 
                        
                        // 5. spjangcd 안씀

                        
                        // 6. 모바일(어차피 다 id 값으로 필터함)
                        "/api/mobile_main/**", "/api/attendance_submit/**", "/api/commute_current/**",
                        "/api/attendance_current/**"
                );
    }

    @Override
    public void addCorsMappings(CorsRegistry registry){
        registry.addMapping("/**")
                .allowedOrigins(
                        "http://localhost:8030", "http://actascld.co.kr:8030/", "http://mes.actascld.co.kr", "https://mes.actascld.co.kr", "https://act.actascld.co.kr",
                        "https://actascld.co.kr", "http://actascld.co.kr",
                        "https://actas-ai.co.kr", "http://actas-ai.co.kr"
                )
                .allowedMethods("GET", "POST", "PUT", "DELETE", "HEAD")
                .allowedHeaders("*")
                .allowCredentials(true);
    }
    

}
