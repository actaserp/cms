package mes.app.aspect;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Before;
import org.springframework.stereotype.Component;
import org.springframework.web.context.request.RequestContextHolder;
import org.springframework.web.context.request.ServletRequestAttributes;

import javax.servlet.http.HttpSession;
import java.lang.reflect.Field;

@Slf4j
@Aspect
@Component
public class TenantSaveAspect {

    @Before("execution(* mes.domain.repository..*.save(..)) && args(entity)")
    public void injectSpjangcd(Object entity) {
        // 1. 보안 인터셉터가 세팅해둔 세션 기반 사업장 코드 가져오기
        String tenantId = TenantContext.get();

        // 2. 인증 정보가 없으면 실행 자체를 차단 (Fail-Fast)
        if (tenantId == null) {

            //웹 요청 컨텍스트가 있는지 확인
            if(RequestContextHolder.getRequestAttributes() == null){
                return; // // 스케줄러 등 웹 요청이 아닌 경우 → 그냥 통과
            }

            ServletRequestAttributes attr = (ServletRequestAttributes) RequestContextHolder.currentRequestAttributes();
            HttpSession session = attr.getRequest().getSession(false);
            if (session != null) {
                tenantId = (String) session.getAttribute("spjangcd");
                TenantContext.set(tenantId); // 복구
            }
        }

        try {
            // 3. 리플렉션을 이용해 엔티티 내부에 spjangcd 필드가 있는지 확인
            Field field = entity.getClass().getDeclaredField("spjangcd");
            field.setAccessible(true);

            // 4. 값이 비어있을 때만 세션의 사업장 코드로 강제 주입
            if (field.get(entity) == null) {
                field.set(entity, tenantId);
            }
        } catch (NoSuchFieldException e) {
            // spjangcd 필드가 없는 공통 테이블 등은 자연스럽게 통과
        } catch (Exception e) {
            log.error("멀티테넌트 데이터 주입 중 오류 발생", e);
        }
    }
}