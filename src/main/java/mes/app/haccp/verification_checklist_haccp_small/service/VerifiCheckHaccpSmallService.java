package mes.app.haccp.verification_checklist_haccp_small.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class VerifiCheckHaccpSmallService {

	@Autowired
	SqlRunner sqlRunner;

	// 결재현황 조회
	public List<Map<String, Object>> getVerifiCheckHaccpSmallApprStatus(String startDate, String endDate, String apprState) {

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
				where b."TableName" = 'verification_checklist_haccp_small'
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
	
	// 점검표 조회
	public Map<String, Object> getVerifiCheckHaccpSmallList(Integer bh_id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bh_id", bh_id);
        
        Map<String,Object> head_info = new HashMap<String,Object>();;
        List<Map<String, Object>> diary_info = new ArrayList<Map<String, Object>>();
        Map<String,Object> items = new HashMap<String,Object>();
        
        if (bh_id > 0) {
        	String sql = """
        		select b.id, b."Char1" as "Title", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate",  coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '작성') as "StateName"
				from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'verification_checklist_haccp_small'
				and b.id = :bh_id
            	""";
        	
      		//일지 헤더	    
            head_info = this.sqlRunner.getRow(sql, dicParam);
            
             sql = """
        		select ir."CheckItem_id" as id
                , ci."ItemGroup1" as group1
                , ci."ItemGroup2" as group2
                , ci."ItemGroup3" as group3
                , ci."Name" as item_name
                , ci."ResultType" as result_type
                , ir."Result1" as result1
                , ir."Result2" as result2
                , ir."Result3" as result3
                , ci._order as index_order
                , ir."CheckResult_id"
                , ir.id as check_result_id
                , ci."minValue" as min_value
                , ci."maxValue" as max_value
                from check_item_result ir
                inner join check_item ci on ci.id = ir."CheckItem_id"
                left join check_result cr on cr.id = ir."CheckResult_id"
                where cr."SourceDataPk" = :bh_id
                order by ci._order
            	""";
             
             diary_info = this.sqlRunner.getRows(sql, dicParam);
             
             
             items.put("head_info", head_info);
             items.put("diary_info", diary_info);
        }
        return items;
	}
	
	//점검표 삭제
	public void deleteVerifiCheckHaccpSmallList(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
            delete from check_item_result                     
            where "CheckResult_id" in (
                select id from check_result 
                where "SourceDataPk" = :bhId
            )
            and "CheckItem_id" in (
	            select distinct ci.id 
	            from check_result cr 
	            inner join check_item  ci
	            on cr."CheckMaster_id" = ci."CheckMaster_id"
	            where cr."SourceDataPk" = :bhId
                )  
			""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		sql = """
	            delete from check_result 
	            where "SourceDataPk" = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
				
		sql = """
			   delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
	}
}
