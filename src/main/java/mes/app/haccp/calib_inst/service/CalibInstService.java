package mes.app.haccp.calib_inst.service;


import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class CalibInstService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 검교정대상 기기 조회
	public List<Map<String, Object>> getCalibInstList(String calibInstName, String tableName) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();                  
        dicParam.addValue("calibInstName", calibInstName);
        dicParam.addValue("tableName", tableName);
        
        String sql = """
			select c.id, c."Name" as calib_inst_name, c."CalibInstClass" as calib_inst_class
	        --, c."CycleBase" as cycle_name, c."CycleNumber" as cycle_number
            , concat(c."CycleNumber", ' 개월') as cycle_name
	        , c."AuthorizedCalibDate" as author_calib_date, c."SelfCalibDate" as self_calib_date, c."NextCalibDate" as next_calib_date
            , DATE_PART('day', cast(c."NextCalibDate" as timestamp) - current_date) as delay_days
	        , c."CalibJudge" as calib_judge, c."Description" as description
	        , c."StartDate" as start_date, c."EndDate" as end_date, c."SourceDataPk" as src_data_pk, c."SourceTableName" as src_table_name
	        from calib_inst c 
	        where 1 = 1
            and c."SourceTableName" = :tableName
        		""";
  			    
	    if (StringUtils.hasText(calibInstName)) {
	    	
	    	sql += " and c.\"Name\" like concat('%%', :calibInstName ,'%%') ";
	    }
	    
        sql += " order by c.\"CalibInstClass\" ";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
	// 검교정대상 기기 상세 조회
	public Map<String, Object> getCalibInstDetailList(Integer id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();                  
        dicParam.addValue("id", id);
        
        String sql = """
        		select c.id, c."Name"
	            , c."CalibInstClass"
	            , c."CycleBase", c."CycleNumber", c."AuthorizedCalibDate", c."SelfCalibDate", c."NextCalibDate", c."CalibJudge", c."Description"
		        , c."StartDate", c."EndDate", c."SourceDataPk", c."SourceTableName"
		        from calib_inst c 
		        where c.id = :id
        		""";

        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
	}
	
	// 검교정내역 조회
	public List<Map<String, Object>> getCalibResultList(Integer calib_inst_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();                  
        dicParam.addValue("calib_inst_id", calib_inst_id);
        
        String sql = """
					select cr.id, c.id as calib_inst_id
		            , c."Name" as calib_inst_name, c."CalibInstClass" as calib_inst_class
		            , cr."CalibDate" as calib_date, cr."Difference" as difference
			        , cr."CalibJudge" as calib_judge
		            , case when cr."CalibJudge" = 'OK' then '합격' else '불합격' end as calib_judge_name
		            , cr."Description" as description
		            , (select "Value" from user_code where "Code" = cr."CalibInstitution" limit 11) as calibInstitution_name
		            , cr."CalibInstitution" as CalibInstitution 
			        from calib_result cr 
		            inner join calib_inst c on c.id = cr."CalibInstrument_id"
			        where cr."CalibInstrument_id" = :calib_inst_id
		            order by cr."CalibDate"
        		""";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
}
