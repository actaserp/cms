package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;


import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class TagService {

	@Autowired
	SqlRunner sqlRunner;
	
	//창고 목록 조회
	public List<Map<String,Object>> getTagResult(String keyword,String tag_group_id, String equipment_id) {
		String tenantId = TenantContext.get();
		MapSqlParameterSource tgParam = new MapSqlParameterSource();
		tgParam.addValue("keyword", keyword);
		tgParam.addValue("tag_group_id",tag_group_id);
		tgParam.addValue("equipment_id",equipment_id);
		tgParam.addValue("spjangcd", tenantId);
		
		String sql = """
			 select t.tag_code
	        , t.tag_name
	        , t.tag_group_id
	        , tg."Name" as tag_group_name
	        , t."Equipment_id" as equipment_id
	        , e."Name" as equipment_name  
            , t."RoundDigit" as round_digit
	        , t."LSL" as lsl
	        , t."USL" as usl
            , dc."Name" as config_name
            from tag t 
            left join tag_grp tg on t.tag_group_id = tg.id
            left join equ e on e.id = t."Equipment_id"
            left join das_config dc on dc.id=t."DASConfig_id"
            where 1=1
            and t.spjangcd=:spjangcd
			""";
		
		
		if (StringUtils.isEmpty(keyword)==false) sql +="and t.tag_name like concat('%%',upper(:keyword),'%%') or t.tag_code like concat('%%',upper(:keyword),'%%')";
		
		if (StringUtils.isEmpty(tag_group_id)==false) sql +="and t.tag_group_id = (:tag_group_id)::int";
		
		if (StringUtils.isEmpty(equipment_id)==false) sql +="and t.\"Equipment_id\" = (:equipment_id)::int";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, tgParam);
		
		return items;
	}
	
	//창고 상세정보 조회
	public Map<String,Object> getTagpResultDetail(String tag_code){
		String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("tag_code", tag_code);
		dicParam.addValue("spjangcd", tenantId);
		String sql = """
			select t.tag_code as id
            , t.tag_code
	        , t.tag_name
	        , t.tag_group_id
	        , tg."Name" as tag_group_name
	        , t."Equipment_id" as equipment_id
	        , e."Name" as equipment_name  
            , t."RoundDigit" as round_digit
	        , t."LSL" as lsl
	        , t."USL" as usl
            , t."DASConfig_id"
            , dc."Name" as config_name
            from tag t 
            left join tag_grp tg on t.tag_group_id = tg.id
            left join equ e on e.id = t."Equipment_id"
            left join das_config dc on dc.id=t."DASConfig_id"
            where 1=1
            and t.tag_code = :tag_code
            and t.spjangcd=:spjangcd
			""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		
		return item;
	}

	public void deleteByTagCode(String tag_code) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("tag_code", tag_code);
        
        
        String sql = """
        		delete from tag
        		where tag_code = :tag_code
        		""";
        
        this.sqlRunner.execute(sql, dicParam);
	}
	
}
