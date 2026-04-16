package mes.config;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
//import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configuration.WebSecurityCustomizer;
import org.springframework.security.config.annotation.web.configurers.AbstractHttpConfigurer;
import org.springframework.security.config.annotation.web.configurers.RequestCacheConfigurer;
import org.springframework.security.web.SecurityFilterChain;
import mes.domain.security.AjaxAwareLoginUrlAuthenticationEntryPoint;

import mes.domain.security.CustomAccessDeniedHandler;
import mes.domain.security.CustomAuthenticationFailureHandler;
import mes.domain.security.CustomAuthenticationManager;
import mes.domain.security.CustomAuthenticationSuccessHandler;
import org.springframework.beans.factory.annotation.Value;


@Configuration
@ComponentScan("mes.domain.security")
public class SecurityConfiguration {
	
	@Autowired
	private CustomAuthenticationSuccessHandler customAuthenticationSuccessHandler;

	@Autowired
	private CustomAuthenticationFailureHandler customAuthenticationFailureHandler;

    @Value("${server.servlet.session.cookie.name}")
    private String sessionCookieName;
		
	@Bean(name="authenticationManager")	
	CustomAuthenticationManager authenticationManager() {
		CustomAuthenticationManager authenticationManager = new CustomAuthenticationManager();
		return authenticationManager;
	}

    @Bean
    SecurityFilterChain filterChain(HttpSecurity http) throws Exception {

        // 1. 헤더 설정
        http
                .headers(headers -> headers
                        .frameOptions(frame -> frame.sameOrigin())
                        .contentTypeOptions(ct -> {})
                        .httpStrictTransportSecurity(hsts -> hsts
                                .includeSubDomains(true)
                                .maxAgeInSeconds(31536000)
                        )
                        .referrerPolicy(referrer -> referrer
                                .policy(org.springframework.security.web.header.writers.ReferrerPolicyHeaderWriter.ReferrerPolicy.STRICT_ORIGIN_WHEN_CROSS_ORIGIN)
                        )
                );

        // 2. CSRF 예외 경로 (기존 pda 및 기타 추가)
        http
                .csrf(csrf -> csrf
                        .ignoringAntMatchers(
                                "/login", "/postLogin",
                                "/api/files/upload/**",
                                "/popbill/webhook",
                                "/pda/**"
                        )
                );

        // 3. 권한 설정 (기존 소스의 모든 permitAll 경로 통합)
        http
                .authorizeRequests(auth -> auth
                        .antMatchers(
                                // 기본 페이지 및 인증
                                "/login", "/logout", "/postLogin", "/intro", "/error", "/alive", "/bill_plan_read", "/biz/save",
                                // 모바일 관련 경로 추가
                                "/mlogin","/api/mobile_main/**",
                                // 정적 리소스
                                "/resource/**", "/img/**", "/images/**", "/js/**", "/css/**",
                                "/assets_mobile/**", "/font/**", "/robots.txt", "/favicon.ico",
                                // PDA 관련
                                "/pda/login", "/pda/app/version/**", "/pda/**",
                                // API 및 외부 연동
                                "/useridchk/**", "/user-auth/**", "/user-auth/save",
                                "/popbill/webhook", "/api/transaction/input/**",
                                "/api/das_device", "/authentication/**", "/api/common/**"
                        ).permitAll()
                        .antMatchers("/setup").hasAuthority("admin")
                        .anyRequest().authenticated()
                );

        // 4. 로그인 설정
        http
                .formLogin(form -> form
                        .loginPage("/login")
                        .loginProcessingUrl("/postLogin")
                        .successHandler(customAuthenticationSuccessHandler)
                        .failureHandler(customAuthenticationFailureHandler)
                        .permitAll()
                );

        // 5. 로그아웃 설정
        http
                .logout(logout -> logout
                        .logoutUrl("/logout")
                        .logoutSuccessUrl("/login")
                        .invalidateHttpSession(true)
                        .deleteCookies(sessionCookieName)
                        .clearAuthentication(true)
                        .permitAll()
                );

        // 6. 예외 처리 및 기타 (기존 소스의 AjaxAware 및 AccessDenied 적용)
        http.httpBasic(httpBasic -> httpBasic.disable());

        http.exceptionHandling(exception -> exception
                .accessDeniedHandler(new CustomAccessDeniedHandler())
                .authenticationEntryPoint(new AjaxAwareLoginUrlAuthenticationEntryPoint("/login"))
        );

        return http.build();
    }
}

