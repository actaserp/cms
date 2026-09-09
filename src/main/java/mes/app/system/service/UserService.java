package mes.app.system.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class UserService {

	@Autowired
	SqlRunner sqlRunner;

	// 사용자 리스트 조회
	public List<Map<String, Object>> getUserList(boolean superUser, Integer group, String keyword, String username, Integer departId, String spjangcd){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("group", group);
		dicParam.addValue("keyword", keyword);
		dicParam.addValue("username", username);
		dicParam.addValue("departId", departId);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
			select au.id
			  , au.first_name
              , up."Name"
              , au.username as login_id
              , up."UserGroup_id"
              , au.email
              , au.tel
              , ug."Name" as group_name
              , up."Factory_id"
              , f."Name" as factory_name
              , d."Name" as dept_name
              , up."Depart_id"
              , up.lang_code
              , au.is_active
              , to_char(au.date_joined ,'yyyy-mm-dd hh24:mi') as date_joined
              , au.spjangcd as spjangcd
              , au.personid as personid
              , p."Code" as personcode
            from auth_user au
            left join user_profile up on up."User_id" = au.id and up.spjangcd = au.spjangcd
            left join user_group ug on ug.id = up."UserGroup_id" and ug.spjangcd = up.spjangcd
            left join factory f on f.id = up."Factory_id" and f.spjangcd = up.spjangcd
            left join depart d on d.id = up."Depart_id" and d.spjangcd = up.spjangcd
            left join person p on p.id = au.personid
            where is_superuser = false
            AND au.spjangcd = :spjangcd
		    """;

		if (superUser != true) {
			sql += "  and ug.\"Code\" <> 'dev' ";
		}

		if (group!=null){
			sql+= " and ug.\"id\" = :group ";
		}

		if (StringUtils.isEmpty(keyword)==false) {
			sql += " and up.\"Name\" like concat('%%', :keyword, '%%') ";
		}

		if (StringUtils.isEmpty(username)==false) {
			sql += " and au.\"username\" = :username ";
		}
		if (departId != null) {
			sql += " and up.\"Depart_id\" = :departId ";
		}

		sql += "order by ug.\"Name\", up.\"Name\"";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}

	// 사용자 상세정보 조회
	public Map<String, Object> getUserDetail(Integer id){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);

		String sql = """
			select au.id
              , up."Name"
              , au.username as login_id
              , au.email
              , ug."Name" as group_name
              , up."UserGroup_id"
              , up."Factory_id"
              , f."Name" as factory_name
              , d."Name" as dept_name
              , up."Depart_id"
              , up.lang_code
              , au.is_active
              , to_char(au.date_joined ,'yyyy-mm-dd hh24:mi') as date_joined
            from auth_user au 
            left join user_profile up on up."User_id" = au.id
            left join user_group ug on up."UserGroup_id" = ug.id 
            left join factory f on up."Factory_id" = f.id 
            left join depart d on d.id = up."Depart_id"
            where au.id = :id
		    """;

		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

		return item;
	}

	// 사용자 그룹 조회
	public List<Map<String, Object>> getUserGrpList(Integer id) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		dicParam.addValue("spjangcd", tenantId);
		String sql = """
        		select ug.id as grp_id
	            , ug."Name" as grp_name
	            ,rd."Char1" as grp_check
	            from user_group ug 
	            left join rela_data rd on rd."DataPk2" = ug.id 
	            and "RelationName" = 'auth_user-user_group' 
	            and rd."DataPk1" = :id
	            where coalesce(ug."Code",'') <> 'dev'
	            and ug.spjangcd = :spjangcd
        		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	/**
	 * 전체 사업장 목록 — 슈퍼유저 사업장 전환용.
	 * 의도적인 크로스테넌트 조회이므로 skip_tenant_check 로 경고를 억제한다.
	 */
	public List<Map<String, Object>> getSpjangList() {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();

		String sql = """
        		/* skip_tenant_check */
        		select spjangcd, spjangnm, saupnum, state
        		  from tb_xa012
        		 order by spjangnm
        		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	/**
	 * 사업장 전환 대상이 실재하는지 확인 (슈퍼유저 전환 API 검증용).
	 * 존재하지 않는 코드로 세션이 오염되는 것을 막는다.
	 */
	public boolean existsSpjang(String spjangcd) {
		if (spjangcd == null || spjangcd.isBlank()) return false;

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
        		/* skip_tenant_check */
        		select spjangcd from tb_xa012 where spjangcd = :spjangcd
        		""";

		return this.sqlRunner.getRow(sql, dicParam) != null;
	}

	public List<Map<String, Object>> getSpjang(String spjangcd) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
        		select spjangcd, spjangnm, saupnum, state
        		  from tb_xa012 where spjangcd = :spjangcd
        		""";

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}



	public List<Map<String, Object>> getPSearchitem(String code, String name, String spjangcd) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("code", code);
		dicParam.addValue("name", name);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
                 select
                 p.id as id
                 , p."Code" as code
                 , p."Name" as name
                 , d."Name" as dept_name
                 , d.id as dept_id
                 from person p
                 left join depart d on d.id = p."Depart_id"
                where
                  p.id::text like concat('%', :code, '%')
                AND p."Name" like concat('%',:name,'%')
                AND p.spjangcd = :spjangcd
                order by p.id
                """;

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}

	public List<Map<String, String>> findspjangcd() {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();

		String sql = """
                SELECT spjangcd, spjangnm
                FROM tb_xa012
            """;
		// SQL 실행
		List<Map<String, Object>> rows = this.sqlRunner.getRows(sql, dicParam);

		List<Map<String, String>> result = rows.stream()
				.map(row -> {
					Map<String, String> map = new HashMap<>();
					map.put("spjangcd", (String) row.get("spjangcd"));
					map.put("spjangnm", (String) row.get("spjangnm"));
					return map;
				})
				.toList();

		return result;
	}

}