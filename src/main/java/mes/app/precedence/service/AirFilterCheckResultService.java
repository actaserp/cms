package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class AirFilterCheckResultService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getApprStat(String startDate, String endDate, String apprStatus) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("apprStatus", apprStatus);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				 select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName", r."LineName"
				 , r."LineNameState", to_char(b."Date1", 'yyyy-MM') as "DataDate"
				 , coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				 , coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				 ,b._creater_id ,up."Name" as creater_name , b._modifier_id, up2."Name" as modifier_name
				 , cm."CheckCycle" , b."Text1"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'master_result_air_filter'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)    
				""";
		
		if (StringUtils.hasText(apprStatus)) {
            if (apprStatus.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprStatus ";
            }
		}
		
		sql += " order by b.\"Date1\" desc, b.id desc  ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getAirFilterRead(Integer bhId, String dataDate, Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		String titleDate = dataDate.substring(0,7);
		
		Integer year = Integer.parseInt(dataDate.substring(0,4));
		
		Integer month = Integer.parseInt(dataDate.substring(5,7));
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("year", year);
		paramMap.addValue("month", month);
		
		Map<String, Object> head_info = new HashMap<>();
		
		String sql = null;
		if (bhId > 0) {
			sql = """
				select bh.id
                , bh."Char1" as "Title"
                , bh."Char2" as "Description"
                , to_char(bh."Date1", 'yyyy-MM') as "DataDate"
                , coalesce(uu."Name", cu."Name") as "FirstName"
                , coalesce(r."State", 'write') as "State"
                , coalesce(r."StateName", '작성') as "StateName"
                from bundle_head bh
                inner join user_profile cu on bh._creater_id = cu."User_id"
                left join user_profile uu on bh._modifier_id = uu."User_id"
                left join v_appr_result r on bh.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where bh."TableName" = 'master_result_air_filter'
                and bh.id = :bhId
				""";
			
			head_info = this.sqlRunner.getRow(sql, paramMap);
		}  else {
			head_info.put("id", 0);
			head_info.put("Title", "공조필터현황("+ titleDate + ")");
			head_info.put("DataDate", titleDate);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("StateName", "작성");
		}
		head_info.put("CheckCycle", "1회/월");
		
		sql = """
			with A as (
            select mt.id
            , mr.id as master_result_id
            , mt."Code" as code
            , mt."Name" as name
            , mt._order as _order
            , mt."Description" as place
            , mt."Type" as type
            , mt."Type2" as type2
            , mr."Char1" as result1
            from master_t mt
            left join master_result mr on mt.id = mr."MasterTable_id"
            and "SourceDataPk" = :bhId and "SourceTableName" = 'bundle_head'
            where mt."MasterClass" = 'air_filter'
            ), B as(
            select A.id as id2, mp.id as check_plan, af.id as file_id1, af2.id as file_id2
            from A
            left join master_year_month_plan mp on A.id = mp."MasterTable_id"
			left join attach_file af on mp.id = af."DataPk" and af."AttachName" = 'replace_filter1'
			left join attach_file af2 on mp.id = af2."DataPk" and af2."AttachName" = 'replace_filter2'
            where mp."DataYear" = :year and mp."DataMonth" = :month and mp."PlanYN" = 'Y'
            )
            select A.*, B.check_plan, B.file_id1, B.file_id2
            from A
            left join B on A.id = B.id2
            order by A.id
			 """;
		
		List<Map<String, Object>> diary_info = this.sqlRunner.getRows(sql, paramMap);
		
		sql = """
			select da.id
            , da."HappenDate" as happen_date
            , mt."Description" as happen_place
            , mt."Type" as type
            , mt."Type2" as type2
            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
            , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name	                
            , up2."Name" as creater_name
            , RIGHT(da."AbnormalDetail", POSITION(':' in REVERSE(da."AbnormalDetail"))-1) as check_name
            , mt._order as _order
            , da."ConfirmDetail" as confirm_detail
            , da."Actor_id" as actor_id
            , da."ActorName" as actor_name
            , da."Confer_id" as confer_id
            , da."ConferName" as confer_name
            , to_char(da._modified,'yyyy-MM-dd') as actor_date
            from v_devi_action da
            inner join master_result mr on mr.id = da."SourceDataPk" and da."SourceTableName" = 'master_result_air_filter'
            and mr."SourceDataPk" = :bhId
            inner join master_t mt on mr."MasterTable_id" = mt.id
            left join user_profile up on up."User_id" = da._modifier_id
            left join user_profile up2 on up2."User_id" = da._creater_id
            order by mt._order
			  """;
		
		List<Map<String, Object>> action_info = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> items = new HashMap<>();
		items.put("head_info", head_info);
		items.put("diary_info", diary_info);
		items.put("action_info", action_info);
		
		return items;
	}

	@Transactional
	public void mstDelete(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
			    delete from master_result
                where "MasterClass" = 'master_t'
                and "SourceDataPk" = :bhId
                and "SourceTableName" = 'bundle_head'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from action_data
                where "TableName" = 'devi_action'
                and "DataPk" in (
                select id from devi_action
                    where "SourceDataPk" not in (
                    select a.id
                    from master_result a
                    left join devi_action b on b."SourceDataPk" = a.id
                    where a."MasterClass" = 'master_t' and a."Char1" = 'X' and a."SourceTableName" = 'bundle_head'
                    )
                and "SourceTableName" = 'master_result_air_filter'
                )
			 """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from devi_action
                where "SourceDataPk" not in (                   
                    select a.id
                    from master_result a
                    left join devi_action b on b."SourceDataPk" = a.id
                    where a."MasterClass" = 'master_t' and a."Char1" = 'X' and a."SourceTableName" = 'bundle_head'
                )
                and "SourceTableName" = 'master_result_air_filter'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			 delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
	}

}
