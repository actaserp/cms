package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class ForeignObjGenResultService {

	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getForeignObj(String masterClass, String baseDate) {
		
		MapSqlParameterSource param = new MapSqlParameterSource();
		param.addValue("masterClass", masterClass);
		param.addValue("baseDate", baseDate);
		
		String sql = """
				select t.id as master_id, t."Name" as master_name
                    , t."Description" as description
                    , t."StartDate" as start_date
                    , t."EndDate" as end_date
	                from master_t t
	                where t."MasterClass" = :masterClass
	                and cast(:baseDate as date) between t."StartDate" and t."EndDate"
                    order by t._order, t."Name"
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, param);
		return items;
	}

	public Map<String, Object> getForeignObjDetail(Integer id, Integer masterId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);
		paramMap.addValue("masterId", masterId);
		
		String sql = """
			select t.id as master_id
            , t."Name"
            , t."Description"
            , t."StartDate", t."EndDate"
            from master_t t
             where t.id = :masterId
			""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);

        return items;
	}

	// 이물발생내역 조회
	public List<Map<String, Object>> getForeignObjResult(String master_class, String data_year) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("master_class", master_class);
		dicParam.addValue("start_date", data_year + "-01-01");
		dicParam.addValue("end_date", data_year + "-12-31");
		
		String sql = """
				select  r.id, t.id as master_id, t."Name" as master_name
	            ,r."DataDate" as data_date, r."DataTime" as data_time, r."Number1" as number1
	            , r."Description" as description
	            from master_t t
	            inner join master_result r on r."MasterTable_id" = t.id 
	            where 1 = 1
	            and r."MasterClass" = :master_class
				""";
		    
		if (StringUtils.isEmpty(data_year) == false) {
			
			sql += " and r.\"DataDate\" between :start_date and :end_date ";
		}
		
		sql += " order by r.\"DataDate\", r.\"DataTime\" ";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);

		return items;
	}

	// 이물발생내역 상세조회
	public Map<String, Object> getForeignObjResultDetail(Integer id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
        		select  r.id, r."MasterTable_id", t."Name"
	            ,r."DataDate", r."DataTime"
	            , r."Number1", r."Number2", r."Number3", r."Number4"
	            , r."Char1", r."Char2", r."Char3", r."Char4"
	            , r."Description"
	            from master_t t
	            inner join master_result r on r."MasterTable_id" = t.id 
	            where r.id = :id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
	}
	
}
