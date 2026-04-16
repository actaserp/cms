package mes.app.precedence.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class UnsuitableProductService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 결재현황 조회
	public List<Map<String, Object>> getUnsuitableProductList(String startDate, String endDate, String apprState,
			String diaryType) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("diaryType", diaryType);
        
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				, cm."CheckCycle" 
				, b."Text1" as diary_type
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'unsuitable_product_result'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (StringUtils.hasText(apprState)) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		if (StringUtils.hasText(diaryType)) {
            	sql += " and b.\"Text1\" = :diaryType ";
		}
		
		sql += " order by b.\"Date1\" desc ";
  			    
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> defectDefect(Integer bh_id, String diary_type, String start_dt, String end_dt, boolean type) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bhId", bh_id);
        dicParam.addValue("startDt", start_dt);
        dicParam.addValue("endDt", end_dt);
        
        Map<String,Object> head_info = new HashMap<String,Object>();;
        List<Map<String, Object>> diary_info = new ArrayList<Map<String, Object>>();
        Map<String,Object> items = new HashMap<String,Object>();
        
    	String sql = """
    		select b.id, b."Char1" as "Title"
    		, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
    		,  coalesce(uu."Name", cu."Name") as "FirstName"
    		, coalesce(r."State", 'write') as "State"
    		, coalesce(r."StateName", '작성') as "StateName"
    		, b."Text1" as diary_type
			from bundle_head b
			inner join user_profile cu on b._creater_id = cu."User_id"
			left join user_profile uu on b._modifier_id = uu."User_id"
			left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
			where b."TableName" = 'unsuitable_product_result'
			and b.id = :bhId
        	""";
    	
  		//일지 헤더	    
        head_info = this.sqlRunner.getRow(sql, dicParam);
        
        if(diary_type.equals("부적합품(입고)")) {
        	if (type) {
        		sql = """
        		  select  tr.id 
				 , mi."InoutDate" as inout_dt
			     , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
			       when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") end as inout_type
			     , fn_code_name('mat_type', mg."MaterialType") as material_type
			     , m."Name" as mat_name
			     , mi."PotentialInputQty" as "potential_input_qty"
			     , ARRAY_TO_STRING(ARRAY_AGG(ti."Name"),' | ') as "testName"
			     , ad."Code" as "result1"
			     , ad."Description" as "result2"
			     , ad."Char1" as "result3"
			     , ad."Char2" as "result4"
			     , ad."Char3" as "result5"
			     , ad."StartDate" as "startDt"
			     , ad."EndDate" as "endDt"
				from test_result tr 
				left join action_data ad on tr.id = ad."DataPk" 
				left join test_item_result tir on tr.id = tir."TestResult_id" 
				left join test_item ti on ti.id = tir."TestItem_id" 
				left join mat_inout mi on mi.id = tr."SourceDataPk" 
				left join material m on m.id = mi."Material_id" 
				left join mat_grp mg on mg.id = m."MaterialGroup_id" 
				where tr."SourceTableName"  = 'mat_inout'
				and  tir."Char1" = 'X'
				""";

        		if (bh_id > 0) {
        			sql += " and ad.\"DataPk2\" = :bhId and ad.\"TableName2\" = 'bundle_head' ";
        		}
        			  
        	} else {
        		sql = """
              		  select  tr.id 
      				 , mi."InoutDate" as inout_dt
      			     , case when mi."InOut" = 'in' then fn_code_name('input_type', mi."InputType") 
      			       when mi."InOut" = 'out' then fn_code_name('output_type', mi."OutputType") end as inout_type
      			     , fn_code_name('mat_type', mg."MaterialType") as material_type
      			     , m."Name" as mat_name
      			     , mi."PotentialInputQty" as "potential_input_qty"
      			     , ARRAY_TO_STRING(ARRAY_AGG(ti."Name"),' | ') as "testName"
      			     , '' as "result1"
      			     , '' as "result2"
      			     , '' as "result3"
      			     , '' as "result4"
      			     , '' as "result5"
      			     , ad."StartDate" as "startDt"
      			     , ad."EndDate" as "endDt"
      				from test_result tr 
      				left join action_data ad on tr.id = ad."DataPk" 
      				left join test_item_result tir on tr.id = tir."TestResult_id" 
      				left join test_item ti on ti.id = tir."TestItem_id" 
      				left join mat_inout mi on mi.id = tr."SourceDataPk" 
      				left join material m on m.id = mi."Material_id" 
      				left join mat_grp mg on mg.id = m."MaterialGroup_id" 
      				where tr."SourceTableName"  = 'mat_inout'
      				and  tir."Char1" = 'X'
      				""";
              		
        	}
        	
	      		if (StringUtils.hasText(start_dt) && StringUtils.hasLength(end_dt)) {
	      			sql += " and mi.\"InoutDate\" between cast(:startDt as date) and cast(:endDt as date) ";
	      		}
	      		
        		sql +=
        		"""
        		group by tr.id,inout_dt,inout_type,material_type,mat_name,"potential_input_qty", "result1", "result2", "result3", "result4", "result5", "startDt", "endDt"
        	    """;
        } else if (diary_type.equals("부적합품(생산)")) {
        	if (type) {
            	sql = """
            			select jrd.id, jr."ProductionDate", jr."WorkOrderNumber", fn_code_name('mat_type', mg."MaterialType") as material_type ,m."Name" as "mat_name" 
    					,jr."OrderQty" ,dt."Name" as "defect_name", jrd."DefectQty" 
					    , ad."Code" as "result1"
					    , ad."Description" as "result2"
					    , ad."Char1" as "result3"
					    , ad."Char2" as "result4"
					    , ad."Char3" as "result5"
    					, ad."StartDate" as "startDt", ad."EndDate" as "endDt"
    					from job_res_defect jrd 
    					left join action_data ad on jrd.id = ad."DataPk" 
    					left join defect_type dt on jrd."DefectType_id"  = dt.id 
    					left join job_res jr on jrd."JobResponse_id" = jr.id
    					left join material m on jr."Material_id"  = m.id 
    					left join mat_grp mg on m."MaterialGroup_id" = mg.id
    					where jrd."DefectQty"  > 0
            		  """;
            	
        		if (bh_id > 0) {
        			sql += " and ad.\"DataPk2\" = :bhId and ad.\"TableName2\" = 'bundle_head' ";
        		}
        		
        	} else {
            	sql = """
            			select jrd.id, jr."ProductionDate", jr."WorkOrderNumber", fn_code_name('mat_type', mg."MaterialType") as material_type ,m."Name" as "mat_name" 
    					,jr."OrderQty" ,dt."Name" as "defect_name", jrd."DefectQty" 
	      			    , '' as "result1"
	      			    , '' as "result2"
	      			    , '' as "result3"
	      			    , '' as "result4"
	      			    , '' as "result5"
    					, ad."StartDate" as "startDt", ad."EndDate" as "endDt"
    					from job_res_defect jrd 
    					left join action_data ad on jrd.id = ad."DataPk" 
    					left join defect_type dt on jrd."DefectType_id"  = dt.id 
    					left join job_res jr on jrd."JobResponse_id" = jr.id
    					left join material m on jr."Material_id"  = m.id 
    					left join mat_grp mg on m."MaterialGroup_id" = mg.id
    					where jrd."DefectQty"  > 0
            		  """;
        	}
        	
    		if (StringUtils.hasText(start_dt) && StringUtils.hasText(end_dt)) {
    			sql += " and jr.\"ProductionDate\" between cast(:startDt as date) and cast(:endDt as date) ";
    		}
    		
        }
        
        diary_info = this.sqlRunner.getRows(sql, dicParam);
        
        
        items.put("head_info", head_info);
        items.put("diary_info", diary_info);
        if (diary_info.size() > 0) {
        	items.put("startDt", diary_info.get(0).get("StartDt"));
        	items.put("endDt", diary_info.get(0).get("endDt"));
        }
        
       return items;
	}

}
