package mes.app.aspect;

import lombok.RequiredArgsConstructor;
import mes.app.notification.BizEvent;
import mes.app.notification.BizEventTrigger;
import mes.domain.entity.User;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionSynchronizationAdapter;
import org.springframework.transaction.support.TransactionSynchronizationManager;

import java.util.Map;

@Aspect
@Component
@RequiredArgsConstructor
public class BizEventAspect {

    private final ApplicationEventPublisher publisher;

    @Around("@annotation(trigger)")
    public Object handleBizEvent(
            ProceedingJoinPoint pjp,
            BizEventTrigger trigger
    ) throws Throwable {

        Object result = pjp.proceed();

        Object targetId = result;

        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        User user = (User) auth.getPrincipal();

        String action = trigger.action();

        // 🔥 payload 기반 action 재판별
        Map<String, Object> payload = null;

        for (Object arg : pjp.getArgs()) {
            if (arg instanceof Map<?, ?> map) {
                payload = (Map<String, Object>) map;
            }
        }

        String spjangcd = user.getSpjangcd();

        TransactionSynchronizationManager.registerSynchronization(
                new TransactionSynchronizationAdapter() {
                    @Override
                    public void afterCommit() {
                        publisher.publishEvent(new BizEvent(
                                trigger.domain(), action, targetId, spjangcd, user.getUsername()
                        ));
                    }
                }
        );

        System.out.println("🔥 BizEventAspect HIT: " + trigger.domain() + " / " + trigger.action());

        return result;
    }

}
