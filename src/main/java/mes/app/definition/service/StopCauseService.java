package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class StopCauseService {

	@Autowired
	SqlRunner sqlRunner;

	// 비가동사유 리스트 조회
	public List<Map<String, Object>> getStopCauseList(String plan_yn, String cause_name) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("plan_yn", plan_yn);
		paramMap.addValue("cause_name", cause_name);
                
    	String sql = """
    		select sc.id 
		        , sc."StopCauseName" as stop_cause_name
		        , c."Value" as plan_yn
		        , sc."Description" as description
	        from stop_cause sc 
	        inner join sys_code c on c."Code" = sc."PlanYN" and c."CodeType" = 'plan_yn'
	        where 1=1
        	""";
    	
    	if (!plan_yn.isEmpty() && plan_yn != null) {
        	sql += " and sc.\"PlanYN\" = :plan_yn ";
		}
    	if (!cause_name.isEmpty() && cause_name != null) {
        	sql += " and upper(sc.\"StopCauseName\") like concat('%%',upper(:cause_name),'%%') ";
		}
		
		sql += " order by id desc ";
				    
	    List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
            
        return items;
	}
	
	// 비가동사유 상세정보 조회
	public Map<String, Object> getStopCauseDetail(Integer id) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
                
    	String sql = """
    		select sc.id 
		        , sc."StopCauseName" as stop_cause_name
		        , sc."PlanYN" as plan_yn
		        , sc."Description" as description
	        from stop_cause sc 
	        where 1=1
	        and sc.id = :id
        	""";
				    
    	Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
            
        return item;
	}
}
