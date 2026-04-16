package mes.app.aspect;

import lombok.extern.slf4j.Slf4j;
import mes.app.util.UtilClass;
import mes.domain.model.AjaxResult;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.stereotype.Component;
import java.util.List;
import java.util.Map;

@Aspect
@Component
@Slf4j
public class DecryptAspect {

    @Around("@annotation(decryptField)")
    public Object decryptResponse(ProceedingJoinPoint joinPoint, DecryptField decryptField) throws Throwable {
        Object result = joinPoint.proceed();

        if (result instanceof List<?>) {
            decryptList((List<?>) result, decryptField);

        } else if (result instanceof AjaxResult ajaxResult) {
            Object data = ajaxResult.data;

            if (data instanceof List<?>) {
                decryptList((List<?>) data, decryptField);

            } else if (data instanceof Map<?, ?> dataMap) {
                Object listObj = dataMap.get("list");
                if (listObj instanceof List<?>) {
                    decryptList((List<?>) listObj, decryptField);
                }
            }
        }

        return result;
    }

    @SuppressWarnings("unchecked")
    private void decryptList(List<?> list, DecryptField decryptField) {
        if (list.isEmpty() || !(list.get(0) instanceof Map<?, ?>)) return;

        String[] columns = decryptField.columns();
        int[] masks = decryptField.masks();

        boolean hasLogged = false;

        for (int i = 0; i < columns.length; i++) {
            String col = columns[i];
            int mask = (masks.length > i) ? masks[i] : 0;

            try {
                UtilClass.decryptEachItem((List<Map<String, Object>>) list, col, mask);
            } catch (Exception e) {
                if (!hasLogged) {
                    log.error("AOP 복호화 실패 - 예: column={}, 이유: {}", col, e.getMessage());
                    hasLogged = true;
                }
                // 그 외 컬럼 실패는 조용히 무시
            }
        }
    }
}
