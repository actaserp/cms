package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ShiftService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getShiftList( String shift_name, String spjangcd) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("shift_name", shift_name);
		dicParam.addValue("spjangcd", spjangcd);

        String sql = """
			select s.id, s."Code" as shift_code
			, s."Name" as shifht_name
			, s."StartTime" as start_time
			, s."EndTime" as end_time 
			, s."Description" as description
			from shift s  
			where 1=1
			AND s.spjangcd = :spjangcd 
            """;
        if (StringUtils.isEmpty(shift_name)==false) sql +="and upper(s.\"Name\") like concat('%%',upper(:shift_name),'%%')";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	
	public Map<String, Object> getShiftDetail(Integer id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
			select s.id 
			, s."Code" 
			, s."Name" 
			, s."StartTime" 
			, s."EndTime" 
			, s."Description" 
			from shift s
			where s.id = :id
            """;

        Map<String, Object> items = this.sqlRunner.getRow(sql, dicParam);
        return items;
	}

}
