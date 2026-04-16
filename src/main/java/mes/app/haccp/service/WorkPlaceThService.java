package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class WorkPlaceThService {

	@Autowired
	SqlRunner sqlRunner;
	
	String[] numPropList = {"temp_low", "temp_upper", "humid_low", "humid_upper"};
	String[] charPropList = {"temp_code", "humid_code", "time1", "time2"};
	
	public List<Map<String, Object>> getWorkPlaceTh(String masterClass, String baseDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("masterClass", masterClass);  
		paramMap.addValue("baseDate", baseDate);
		
	    	    		
		String numFuncName = "fn_prop_data_number";
		String charFuncName = "fn_prop_data_char";
		
		String sql = """
				select c.id
            , c."MasterClass" as master_class, c."Code" as code
            , c."Name" as name
            , c."Type" as code_type
            , c."Type2" as code_type2
            , c."Description" as description
            , c."Char1" as char1
            , c."Number1" as number1
            , c."Date1" as date1
            , c."Text1" as text1
            , c."StartDate" as start_date
            , c."EndDate" as end_date
				""";
		
		for (String code: numPropList) {
			sql += "," + numFuncName + "('master_t', c.id, '" + code + "' ) as " + code;
		}
		
		for (String code: charPropList) {
			sql += "," + charFuncName + "('master_t', c.id, '" + code + "' ) as " + code;
		}
		
		sql += """
			    from master_t c
	            where c."MasterClass" = :masterClass
	            and cast(:baseDate as date) between coalesce(c."StartDate",'2000-01-01') and coalesce(c."EndDate",'2100-12-31')
	            order by c._order, c."Name"
			   """;
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> getWorkPlaceThDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);
		
		String numFuncName = "fn_prop_data_number";
		String charFuncName = "fn_prop_data_char";
		
		String sql = """
				  select c.id
	            , c."MasterClass", c."Code"
	            , c."Name"
	            , c."Type"
	            , c."Type2"
	            , c."Description"
	            , c."Char1"
	            , c."Number1"
	            , c."Date1"
	            , c."Text1"
	            , c."StartDate"
	            , c."EndDate"
				""";
		for (String code: numPropList) {
			sql += "," + numFuncName + "('master_t', c.id, '" + code + "' ) as " + code;
		}
		
		for (String code: charPropList) {
			sql += "," + charFuncName + "('master_t', c.id, '" + code + "' ) as " + code;
		}
		
        sql += " from master_t c where C.id = :id ";
        
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}

	public List<Map<String, Object>> readResultWorkPlaceTh(String masterClass, String baseDate) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("masterClass", masterClass);  
		paramMap.addValue("baseDate", baseDate);
		
		String sql = """
				select t.id as master_id, t."Name" as master_name
	            , r.id, r."DataDate" as data_date, r."DataTime" as data_time, r."Number1" as temper, r."Number2" as humidity
	            , r."Description" as description
	             from master_t t
	             left join master_result r on r."MasterTable_id" = t.id 
	             and r."DataDate" = cast(:baseDate as date)
	             where t."MasterClass" = :masterClass
	             and cast(:baseDate as date) between t."StartDate" and t."EndDate"
	             order by t._order, t."Name", r."DataDate", r."DataTime"
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> detailResultWorkPlaceTh(Integer id, Integer masterId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		paramMap.addValue("masterId", masterId);
		
		String sql = """
				select t.id as master_id
	            , t."Name" as master_name
	            , r.id, r."DataDate", r."DataTime", r."Number1" as "Temper", r."Number2" as "Humidity"
	            , r."Description"
	            from master_t t
	            left join master_result r on r."MasterTable_id" = t.id 
	             and r.id = :id
	             where t.id = :masterId
				""";
		
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}

}
