package mes.app.system.service;

import java.util.List;
import java.util.Map;

import mes.domain.entity.User;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;
@Service 
public class UserGroupService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String,Object>> getUserGroupList(Boolean super_user, String spjangcd) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
			select id, "Code" as code 
            , "Name" as name 
            , "Description" as description 
            , "Disabled" as disabled
            , case when exists (select 1 FROM user_profile up WHERE up."UserGroup_id" = ug.id) THEN 'Y' else 'N' end as flag
            , "gmenu" as gmenu 
            , mi."MenuName" as gmenuname
            , to_char(ug."_created" ,'yyyy-mm-dd hh24:mi:ss') as created
            from user_group ug 
            left join menu_item mi
			on mi."MenuCode" = ug.gmenu
            where 1 = 1
            and ug.spjangcd = :spjangcd
			""";
				
			
		if (super_user == false) {
			sql += " and \"Code\" <> 'dev' ";
		}
			
		
		sql += " order by \"Name\" ";
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
		
	}
	
	public Map<String, Object> getUserGroup(int id) {
		String sql = """
			select id, "Code" as code 
            ,"Name" as name 
            ,"Description" as description 
            ,"Disabled" as disabled 
            ,to_char("_created" ,'yyyy-mm-dd hh24:mi:ss') as created
            from user_group 
            where id = :group_id
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("group_id", id);
		
		Map<String , Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}

	public Map<String, Object> getDefaultMenu(User user) {

		String sql = """
			select u."gmenu", m."MenuName"
			from user_group u
			join menu_item m on u.gmenu = m."MenuCode"
            where id = :group_id
			""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("group_id", user.getUserProfile().getUserGroup().getId());

		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
	
}