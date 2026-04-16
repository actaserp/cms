package mes.app.haccp.service;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class EduYearPlanService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 결재현황 조회
	public List<Map<String, Object>> getEduYearPlanApprStatus(String startDate, String endDate, String apprState) {

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
				, b."Text1" as "DataYear"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'edu_year_target'
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
	
	// 교육계획 조회
	public Map<String, Object> getEduYearPlanList(Integer bh_id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("bh_id", bh_id);
        
        Map<String,Object> head_info = new HashMap<String,Object>();;
        List<Map<String, Object>> diary_info = new ArrayList<Map<String, Object>>();
        Map<String,Object> items = new HashMap<String,Object>();
        
        if (bh_id > 0) {
        	String sql = """
        		select b.id
        		, b."Char1" as "Title"
        		, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
        		, coalesce(uu."Name", cu."Name") as "FirstName"
        		, coalesce(r."State", 'write') as "State"
        		, coalesce(r."StateName", '작성') as "StateName"
        		, b."Text1" as "DataYear"
				from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'edu_year_target'
				and b.id = :bh_id
            	""";
        	
      		//일지 헤더	    
            head_info = this.sqlRunner.getRow(sql, dicParam);
            
             sql = """
        		select t.id as edu_year_target_id
        		, t.\"EduTitle\" as edu_title
        		, t.\"EduTarget\" as edu_target
        		, t.\"EduContent\" as edu_content
        		, t.\"Remark\" as remark
            	""";
     		
     		for (int i = 1; i < 13; i++) {
     			sql += " , min(case when p.\"DataMonth\" = " + i + " then p.id end) as pid_" + i + " ";
     			sql += " , min(case when p.\"DataMonth\" = " + i + " then p.\"PlanYN\" end) as plan_" + i + " ";
     		}
     		
     		sql += """
     			    from edu_year_target t 
     		        left join edu_year_month_plan p on p."EduYearTarget_id" = t.id
     		        left join rela_data rd on rd."DataPk2" = t.id
     		        where rd."DataPk1" = :bh_id
     		        group by t."id", t."EduTitle", t."EduTarget", t."EduContent", t."_order"
     	            order by t._order
     				""";
             
             diary_info = this.sqlRunner.getRows(sql, dicParam);
             
             items.put("head_info", head_info);
             items.put("diary_info", diary_info);
        }
        return items;
	}
	
	public List<Map<String, Object>> getEduYearPlan(String dataYear) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dataYear", dataYear);
		
		String sql = "  select t.id as edu_year_target_id, t.\"EduTitle\" as edu_title, t.\"EduTarget\" as edu_target, t.\"EduContent\" as edu_content ";
		
		for (int i = 1; i < 13; i++) {
			sql += " , min(case when p.\"DataMonth\" = " + i + " then p.id end) as pid_" + i + " ";
			sql += " , min(case when p.\"DataMonth\" = " + i + " then p.\"PlanYN\" end) as plan_" + i + " ";
		}
		
		sql += """
			    from edu_year_target t 
		        left join edu_year_month_plan p on p."EduYearTarget_id" = t.id
		        where t."DataYear" = cast(:dataYear as Integer)
		        group by t."id", t."EduTitle", t."EduTarget", t."EduContent", t."_order"
	            order by t._order
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

}
