package mes.app.notification;

import lombok.RequiredArgsConstructor;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

@Service
@RequiredArgsConstructor
public class NotificationTargetService {

    private final SqlRunner sqlRunner;

    public List<String> findReceivers(String domain, String spjangcd) {

        String sql = """
            SELECT DISTINCT au.username
            FROM user_group_menu ugm
            JOIN user_group ug
              ON ugm."UserGroup_id" = ug.id
            JOIN user_profile up
              ON ug.id = up."UserGroup_id"
            JOIN auth_user au
              ON up."User_id" = au.id
            WHERE ugm."MenuCode" = :domain
              AND ugm."AuthCode" LIKE '%A%'
              AND au.spjangcd = :spjangcd

        """;

        MapSqlParameterSource params = new MapSqlParameterSource();
        params.addValue("domain", domain);
        params.addValue("spjangcd", spjangcd);

        List<Map<String, Object>> rows = sqlRunner.getRows(sql, params);

        return rows.stream()
                .map(row -> row.get("username").toString())
                .toList();
    }
}
