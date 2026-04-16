package mes.app.test.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class TestMethodService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getTestMethodList(String keyword){
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("keyword", keyword);
		dicParam.addValue("spjangcd", tenantId);

        String sql=""" 
                select tm.id
                , tm."Code" as test_method_code 
                , tm."Name" as test_method_name 
                , eg."Name" as equip_group_name
                from test_method tm 
                left join equ_grp eg on eg.id = tm."EquipmentGroup_id"
                where 1 = 1
                and tm.spjangcd = :spjangcd
        """;
        if (StringUtils.hasText(keyword)){
             sql += """ 
             and tm."Name" like concat('%%',:keyword,'%%')
             """;
        }
        sql += " order by 2 ";

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	
	public Map<String, Object> getTestMethodDetail(int tm_id){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("tm_id", tm_id);
		String sql = """
            select tm.id
            , tm."Code" as test_method_code 
            , tm."Name" as test_method_name
            , tm."Description" as description 
            , eg.id as equip_group_id
            , eg."Name" as equip_group_name
            from test_method tm 
            left join equ_grp eg on eg.id = tm."EquipmentGroup_id"
            where tm.id = :tm_id
		""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
	public List<Map<String, Object>> getTestItemList(int tm_id){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("tm_id", tm_id);
		String sql = """
            select id, "Code" as test_item_code, "Name" as test_item_name
	        from test_item 
	        where "TestMethod_id" = :tm_id
	        order by 2
		""";
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	

}
