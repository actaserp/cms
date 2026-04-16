package mes.app.system.service;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class LoginLogService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getLoginLogList(Timestamp start, Timestamp end, String keyword, String type) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", tenantId);
		dicParam.addValue("start", start);
		dicParam.addValue("end", end);
		dicParam.addValue("keyword", keyword);

		String sql = """
            select row_number() over (order by date_trunc('day', ll._created) desc, up."Name" asc, ll._created desc) as row_number
            , ll.id
            , ll."Type" as type
            , ll."IPAddress" as addr
            , au.username as login_id
            , up."Name" as name
            , to_char(ll."_created" ,'yyyy-mm-dd hh24:mi:ss') as created 
            from login_log ll 
            left join auth_user au ON au.id = ll."User_id" 
            left join user_profile up on up."User_id" = ll."User_id" 
            where ll._created between :start and :end
            and ll.spjangcd = :spjangcd
            """;

		// 'login', 'logout' 타입을 적용할 경우 필터 추가
		if (StringUtils.isNotEmpty(type)) {
			sql += " and ll.\"Type\" = :type ";
			dicParam.addValue("type", type);
		} else {
			sql += " and (ll.\"Type\" = 'login' or ll.\"Type\" = 'logout')"; // 전체 조회 시
		}

		// 키워드 검색 추가 조건
		if (StringUtils.isNotEmpty(keyword)) {
			sql += """ 
                and (au.username ilike concat('%%', :keyword, '%%') 
                    or up."Name" ilike concat('%%', :keyword, '%%') 
                    )
                """;
		}

		// 정렬 조건은 항상 동일하게 적용
		sql += " order by date_trunc('day', ll._created) desc, up.\"Name\" asc, ll._created desc ";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}
}
