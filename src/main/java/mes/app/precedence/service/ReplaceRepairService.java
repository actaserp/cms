package mes.app.precedence.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ReplaceRepairService {

	@Autowired
	SqlRunner sqlRunner;

	// 보수내역관리 조회
	public List<Map<String, Object>> getMyFactoryList(Integer master_table_id, String date_from, String date_to) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("date_from", Date.valueOf(date_from));   
        dicParam.addValue("date_to", Date.valueOf(date_to));
        dicParam.addValue("master_table_id", master_table_id);
        
        String sql = """
        		select mr.id, mr."DataDate" as data_date, mr."Number1" as worker_count
	            , mr."Char1" as work_content, mr."Char2" as field_tool
	            , mr."Char3" as work_result
	            , mr."Char4" as work_after_confirm
	            , mr."Description" as description
		        from master_result mr
		        inner join master_t m on m.id = mr."MasterTable_id"
		        where m.id = :master_table_id
		        and mr."DataDate" between :date_from and :date_to
		        order by mr."DataDate"
        		""";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}

	// 보수내역관리 상세조회
	public Map<String, Object> getMyFactoryDetailList(Integer master_table_id, Integer id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();     
        dicParam.addValue("master_table_id", master_table_id);    
        dicParam.addValue("id", id);
        
        String sql = """
        		select mr.id, mr."DataDate" as data_date, mr."Number1" as worker_count
		        , mr."Char1" as work_content, mr."Char2" as field_tool
		        , mr."Char3" as work_result
		        , mr."Char4" as work_after_confirm
		        , mr."Description" as description
		        from master_result mr
		        inner join master_t m on m.id = mr."MasterTable_id"
		        where m.id = :master_table_id
		        and mr.id = :id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
}
