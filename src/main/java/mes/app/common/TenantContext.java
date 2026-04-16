package mes.app.common;

import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpSession;

public class TenantContext {
    private static final ThreadLocal<String> currentTenant = new ThreadLocal<>();

    public static void set(String tenantId) {
        currentTenant.set(tenantId);
    }

    public static String get() {
        String tenantId = currentTenant.get();

        // [복구 로직] ThreadLocal이 비어있다면 세션에서 재획득 시도
        if (tenantId == null) {
            try {
                ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.getRequestAttributes();
                if (attr != null) {
                    HttpSession session = attr.getRequest().getSession(false);
                    if (session != null) {
                        tenantId = (String) session.getAttribute("spjangcd");
                        set(tenantId); // 다음 호출을 위해 다시 세팅
                    }
                }
            } catch (Exception e) {
                // HTTP 요청 밖(스케줄러 등)에서 호출될 경우 대비
            }
        }
        return tenantId;
    }

    public static void clear() {
        currentTenant.remove();
    }
}