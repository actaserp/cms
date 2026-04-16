package mes.app.interceptor;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

@Component
@Slf4j
public class SpjangSecurityInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        String uri = request.getRequestURI();

        // API 요청이 아니면 통과
        if (!uri.contains("/api/")) return true;

        HttpSession session = request.getSession(false);

        if (session == null || session.getAttribute("spjangcd") == null) {
            response.sendError(HttpServletResponse.SC_UNAUTHORIZED, "로그인이 필요합니다.");
            return false;
        }

        // 2. userid 조작 체크
        String sessionUserId = String.valueOf(session.getAttribute("userid"));
        String paramUserId = request.getParameter("userid");

        if (paramUserId != null && !paramUserId.equals(sessionUserId)) {
            log.warn("[보안위반 차단] userid 조작 시도 - SessionUser: {}, Param: {}, URI: {}",
                    sessionUserId, paramUserId, uri);
            response.sendError(HttpServletResponse.SC_FORBIDDEN, "잘못된 접근입니다.");
            return false;
        }

        String sessionSpjangcd = (String) session.getAttribute("spjangcd");
        String paramSpjangcd = request.getParameter("spjangcd");

        // 2. [강력한 격리] 파라미터 조작 절대 불가
        // 슈퍼유저든 누구든, 파라미터로 들어온 값이 세션의 사업장 코드와 다르면 차단합니다.
        if (paramSpjangcd != null && !paramSpjangcd.equals(sessionSpjangcd)) {
            log.warn("[보안위반 차단] 파라미터 조작 시도 - User: {}, Session: {}, Param: {}, URI: {}",
                    session.getAttribute("userid"), sessionSpjangcd, paramSpjangcd, uri);

            response.sendError(HttpServletResponse.SC_FORBIDDEN, "잘못된 접근입니다. (사업장 코드 불일치)");
            return false;
        }

        // 3. 검증된 사업장 코드를 전역 컨텍스트에 저장
        // SqlRunner의 checkTenantSafety에서 이 값을 기준으로 검사합니다.
        TenantContext.set(sessionSpjangcd);

        return true;
    }

    @Override
    public void afterCompletion(HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
        // 쓰레드 로컬 클린업
        TenantContext.clear();
    }
}