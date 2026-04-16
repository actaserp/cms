package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class EquipHistoryService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 결재현황 조회
	public List<Map<String, Object>> getEquipHistoryApprStatus(String startDate, String endDate, String apprState) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("apprState", apprState);
        
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				, cm."CheckCycle" 
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'equip_history'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (!apprState.isEmpty() && apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		sql += " order by b.\"Date1\" desc ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// 설비변동사항 조회
	public Map<String, Object> getEquipHistoryList(Integer bh_id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bh_id", bh_id);
        
        Map<String,Object> head_info = new HashMap<String,Object>();;
        Map<String,Object> diary_info = new HashMap<String,Object>();;
        Map<String,Object> items = new HashMap<String,Object>();
        
        if (bh_id > 0) {
        	String sql = """
        		select b.id
        		, b."Char1" as "Title"
        		, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
        		, coalesce(uu."Name", cu."Name") as "FirstName"
        		, coalesce(r."State", 'write') as "State"
        		, coalesce(r."StateName", '작성') as "StateName"
				from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'equip_history'
				and b.id = :bh_id
            	""";
        	
      		//일지 헤더	    
            head_info = this.sqlRunner.getRow(sql, dicParam);
            
             sql = """
	        		select eh.id
	                , eq."Name" as equipment_name
	                --, eh."DataDate" as datadate
	                , eh."Description" as description
	                , eh."Content" as content
	                , eh."Cost" as cost
	                , eq.id as equ_id
	                , eh."Char1" as manager
	                , eh."Char2" as part_leader
	                from equip_history eh
	                inner join equ eq on eh."Equipment_id" = eq.id
	                where eh."ApprDataPk" = :bh_id
	                and eh._status = 'history'
            	""";
     		
             diary_info = this.sqlRunner.getRow(sql, dicParam);
             
             items.put("head_info", head_info);
             items.put("diary_info", diary_info);
        }
        return items;
	}

	// 설비변동사항 상세조회
	public Map<String, Object> getEquipHistoryDetailList(Integer id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
        		select eh.id
                , eq."id" as equ_id
                , eq."Name" as equipment_name
                , eh."DataDate" as datadate
                , eh."Description" as description
                , eh."Content" as content
                , eh."Cost" as cost
                , eh."Char1" as manager
                , eh."Char2" as part_leader
                from equip_history eh
                inner join equ eq on eh."Equipment_id" = eq.id
                where eh.id = :id
                and eh._status = 'history'
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);

        return item;
	}
}
