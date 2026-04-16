package mes.app.interceptor;

import lombok.extern.slf4j.Slf4j;
import mes.app.common.TenantContext;
import org.hibernate.resource.jdbc.spi.StatementInspector;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class TenantSqlInspector implements StatementInspector {

    @Override
    public String inspect(String sql) {
        String tenantId = TenantContext.get();

        // 1. 인증 정보가 없거나 시스템 계정인 경우 검사 패스
        if (tenantId == null) return sql;

        String lowSql = sql.toLowerCase();

        // 2. 화이트리스트 테이블 (격리 제외 대상)
        if (lowSql.contains("menu_use_log")
                || lowSql.contains("sys_option")
                || lowSql.contains("label_code")
                || lowSql.contains("label_code_lang")
                || lowSql.contains("sys_code")
        ) {
            return sql;
        }

        // 3. 보안 검증: DML(SELECT, UPDATE, DELETE) 쿼리인데 spjangcd가 없는 경우
        if (lowSql.contains("select") || lowSql.contains("update") || lowSql.contains("delete")|| lowSql.contains("insert")) {

            if (lowSql.matches(".*delete from \\w+ where id=\\?.*") ||
                    lowSql.matches(".*update \\w+ set .* where id=\\?.*")) return sql;

            if (!lowSql.contains("spjangcd") && !lowSql.contains("/* skip_tenant_check */")) {

                // 쿼리를 수정하지 않고, 위반 사항만 명확히 기록합니다.
                log.warn("⚠️ [Multi-Tenant Security] 격리 조건(spjangcd) 누락 감지 (JPA/Hibernate): {}", sql);

                // 여기서 throw new SecurityException()을 던지면 바로 차단 모드가 됩니다.
            }
        }

        return sql; // 쿼리는 변형 없이 원본 그대로 반환
    }
}