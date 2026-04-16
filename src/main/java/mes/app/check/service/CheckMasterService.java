package mes.app.check.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class CheckMasterService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 점검등록 조회 
	public List<Map<String, Object>> getCheckMast(String srch_check_name) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srch_check_name", srch_check_name);
		
        String sql = """
        		select c.id as id
	            , c."Name" as check_name
	            , c."CheckClassCode" as check_class_code
	            , c."Code" as code
	            , c."ChargerDepart" as charger_department
	            , c."CheckCycle" as check_cycle
	            , c."Description" as description
	            from check_mast c
	            where 1=1
        		""";
  		
	    if (StringUtils.isEmpty(srch_check_name) == false) {
	    	sql +=" and c.\"Name\" like concat('%%', :srch_check_name,'%%') ";
	    }
	    
        sql += " order by c.\"Name\" ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	// 점검등록 상세조회
	public Map<String, Object> getCheckMastDetail(Integer check_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_id", check_id);
		
        String sql = """
        		select c.id
	            , c."Name" as check_name
	            , c."CheckClassCode" as check_class_code
	            , c."Code" as code
	            , c."ChargerDepart" as charger_department
	            , c."CheckCycle" as check_cycle
	            , c."Description" as description
	            from check_mast c
	            where c.id = :check_id
        		""";
  		       
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}
	
	
}
