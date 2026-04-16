package mes.app.haccp.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class YearVerificationPlanService {

	@Autowired
	SqlRunner sqlRunner;
	
	public Map<String, Object> YearVerificationPlan(String dataYear,Integer bhId,Authentication auth,String dataDate) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dataYear", dataYear);
		paramMap.addValue("bhId", bhId);
		
		User user = (User)auth.getPrincipal();
		Map<String, Object> head_info = new HashMap<>();
		Map<String, Object> items = new HashMap<>();
		
		String sql = null;
		
		
		if(bhId != 0) {
			sql = "  select t.id as year_verification_plan_id, t.\"VerificationTarget\" as verification_target, t.\"VerificationMethod\" as verification_method";
			
			for (int i = 1; i < 13; i++) {
				sql += " , min(case when p.\"DataMonth\" = " + i + " then p.id end) as pid_" + i + " ";
				sql += " , min(case when p.\"DataMonth\" = " + i + " then p.\"PlanYN\" end) as plan_" + i + " ";
			}
			
			sql += """
				    from year_verification_plan t 
				    left join rela_data rd on rd."DataPk2" = t.id
			        left join year_verification_month_plan p on p."YearVerPlanTargetId" = t.id
			        where 1=1 
			        AND t."DataYear" = cast(:dataYear as Integer)
			        AND rd."DataPk1" = :bhId
			        group by t."id", t."VerificationTarget", t."VerificationMethod",t."_order"
		            order by t._order
					""";
			
			List<Map<String, Object>> diary_info = this.sqlRunner.getRows(sql, paramMap);
			
			items.put("head_info",diary_info);
			
		}else {
			head_info.put("id", 0);
			head_info.put("Title", "설비이력카드 일지");
			head_info.put("DataDate", dataDate);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("State", "write");
			head_info.put("StateName", "작성");
			
			items.put("head_info",head_info);
		}
		return items;
	}

	public void deletePlan(Integer yearVerPlanTargetId) {
		
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("yearVerPlanTargetId", yearVerPlanTargetId);
		
		String sql = """
					delete from year_verification_month_plan where "YearVerPlanTargetId" = :yearVerPlanTargetId
				
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		String sql1 ="""
				delete from rela_data where 1=1 AND "DataPk2" = :yearVerPlanTargetId AND "TableName2" = 'year_verification_plan'
				""";
		
		this.sqlRunner.execute(sql1, paramMap);
		
		
	}

	public List<Map<String, Object>> getList(String start_date, String end_date) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("start_date", start_date);
		paramMap.addValue("end_date", end_date);
		
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
				where b."TableName" = 'year_verification_plan'
				and b."Date1" between cast(:start_date as date) and cast(:end_date as date)
				order by b.\"Date1\" desc
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		
		return items;
	}

	public void deleteList(Integer id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = null;
		
		sql = """
				delete from bundle_head 
				where 1=1 
				AND id = :id
				AND "TableName" = 'year_verification_plan'
				""";
		
		this.sqlRunner.execute(sql, paramMap);
	
		
		sql ="""
				delete from rela_data 
				where 1=1 
				AND "DataPk1" = :id AND "TableName1" = 'bundle_head' AND "TableName2" = 'year_verification_plan'
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		
		
		sql = """
				with T1 as (select yp.* from year_verification_plan yp
					left join rela_data rd on rd."DataPk2" = yp.id
					where 1=1
					AND rd."TableName2" = 'year_verification_plan'
					AND rd."DataPk1" = :id)
					delete from year_verification_month_plan where "YearVerPlanTargetId" = T1.id
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		sql = """
					delete from year_verification_plan yp
					left join rela_data rd on rd."DataPk2" = yp.id
					where 1=1
					AND rd."TableName2" = 'year_verification_plan'
					AND rd."DataPk1" = :id
				
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		
		
		
		
		
	}

	public void deleteRelaData(Integer paramBhid) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("paramBhid", paramBhid);
		
		String sql = """
				delete from rela_data where 1=1 AND "DataPk1" = :paramBhid AND TableName1 = 'bundle_head'
				""";
		this.sqlRunner.execute(sql, paramMap);
		
		String sql2 ="""
				with T1 as (
					select yp.* 
					from year_verification_plan yp
					left join rela_data rd on rd."DataPk2" = yp.id
					where 1=1
					AND rd."TableName2" = 'year_verification_plan'
					AND rd."DataPk1" = :paramBhid)
					delete from year_verification_month_plan where "YearVerPlanTargetId" = T1.id
				""";
		
		this.sqlRunner.execute(sql2, paramMap);
		
		
		String sql1 = """
				delete from year_verification_plan yp
					left join rela_data rd on rd."DataPk2" = yp.id
					where 1=1
					AND rd."TableName2" = 'year_verification_plan'
					AND rd."DataPk1" = :paramBhid
				
				""";
		this.sqlRunner.execute(sql1, paramMap);
		
		
		
		
	}

}
