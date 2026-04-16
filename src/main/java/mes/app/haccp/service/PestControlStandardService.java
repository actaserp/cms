package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class PestControlStandardService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getPestControlList(String haccpAreaClassCode, String pestClassCode,
			String seasonCode) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("haccpAreaClassCode", haccpAreaClassCode);  
		paramMap.addValue("pestClassCode", pestClassCode);  
		paramMap.addValue("seasonCode", seasonCode); 
		
		String sql = """
				select pcs.id, pcs."HaccpAreaClassCode" as haccp_area_class_code
                , pcs."PestClassCode", pcs."SeasonCode"
	            , fn_code_name('haccp_area_class', pcs."HaccpAreaClassCode") haccp_area_class
	            , fn_code_name('pest_class', pcs."PestClassCode") as pest_class
	            , fn_code_name('pest_season', pcs."SeasonCode") as season
	            , pcs."FromCount" as from_count, pcs."ToCount" as to_count, pcs."ActionContent" as action_content
	            from pest_control_standard pcs
	            where 1 = 1
				""";
		
		if(!haccpAreaClassCode.isEmpty()) sql += " and pcs.\"HaccpAreaClassCode\" = :haccpAreaClassCode ";
		if(!pestClassCode.isEmpty()) sql += " and pcs.\"PestClassCode\" = :pestClassCode ";
		if(!seasonCode.isEmpty()) sql += " and pcs.\"SeasonCode\" = :seasonCode ";
		
		sql += "  order by pcs.\"HaccpAreaClassCode\", pcs.\"PestClassCode\", pcs.\"SeasonCode\" ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> getPestControlDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);  
		
		String sql = """
				select pcs.id, pcs."HaccpAreaClassCode" 
                , pcs."PestClassCode", pcs."SeasonCode"
                , fn_code_name('haccp_area_class', pcs."HaccpAreaClassCode") as haccp_area_class
                , fn_code_name('pest_class', pcs."PestClassCode") as pest_class
                , fn_code_name('pest_season', pcs."SeasonCode") as season
                , pcs."FromCount", pcs."ToCount", pcs."ActionContent"
                from pest_control_standard pcs
                where id = :id
				""";
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}
	
	
}
