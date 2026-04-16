package mes.app.check.service;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class CheckTargetService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 점검대상 조회
	public List<Map<String, Object>> getCheckTarget(Integer check_master_id, String check_date) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("check_master_id", check_master_id);
		paramMap.addValue("check_date", Date.valueOf(check_date));
		
        String sql = """
        		select ct.id
	            , c."Name" as check_master_name
	            , ct."TargetGroup1" as group1
	            , ct."TargetGroup2" as group2
	            --, ct."TargetGroup3" as group3
	            , ct."TargetName" as target_name
	            , ct."StartDate" as start_date
	            , ct."EndDate" as end_date
	            , ct."_order" as index_order
	            from check_target ct
	            inner join check_mast c on c.id = ct."CheckMaster_id"
	            where 1=1
        		""";
  		
	    if (check_master_id != null) {
	    	sql +=" and c.id = :check_master_id ";
	    }
	    
        sql += """
        		and ct."StartDate" <= :check_date and ct."EndDate" >= :check_date
        		order by ct._order desc, Ct."TargetName" desc
        		""";
       
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	// 점검대상 상세조회
	public Map<String, Object> getCheckTargetDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
        String sql = """
        		select  ct.id
	            , c.id as check_master_id
	            , ct."TargetGroup1" as group1
	            , ct."TargetGroup2" as group2
	            --, ct."TargetGroup3" as group3
	            , ct."TargetName" as target_name
	            , ct."StartDate" as start_date
	            , ct."EndDate" as end_date
	            from check_target ct
	            inner join check_mast c on c.id = ct."CheckMaster_id"
	            where ct.id = :id
        		""";
  		       
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}
}
