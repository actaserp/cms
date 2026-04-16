package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;

import mes.domain.entity.CheckMaster;
import mes.domain.entity.User;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.services.SqlRunner;

@Service
public class AirConfresiaService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;

	public Map<String, Object> getApprStat(String startDate, String endDate, String apprState) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '작성') as "StateName", r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN",
				 coalesce(r."EditYN", 'Y') as "EditYN", coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
                ,b._creater_id ,up."Name" as creater_name , b._modifier_id, up2."Name" as modifier_name, cm."CheckCycle", b."Text1"
                ,b."Char2" as "CheckStep"
                from bundle_head b                               
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                left join user_profile up on b._creater_id = up."User_id"
                left join user_profile up2 on b._modifier_id = up2."User_id"
                left join check_mast cm on cm.id = b."Number1"
                where b."TableName" = 'check_result_cross_contamination1'
                and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		sql += """
				order by b."Date1" desc , b.id desc
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("document_info", items);
		
		return item;
	}

	public Map<String, Object> getApprStatRead(Integer bhId, String data_Date, Authentication auth) {
		User user = (User)auth.getPrincipal();
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("dataDate", data_Date);
		paramMap.addValue("bhId", bhId);		
		
		//CheckMaster cm = new CheckMaster();
		
		Map<String, Object> items = new HashMap<>();
		
		String sql = null;
		
		Map<String, Object> head_info = new HashMap<>();
		Map<String, Object> diary_grp_info = new HashMap<>();
		List<Map<String,Object>> diary_info = null;
		List<Map<String,Object>> action_info = null;
		
		if(bhId > 0) {
			sql ="""
                select b.id, b."Char1" as "Title", to_char(b."Date1", 'yyyy-MM') as "DataDate",  coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '작성') as "StateName"  , b."Text1"
                ,cr.id as check_result_id ,cr."CheckMaster_id", cr."Description", cm."Name" as check_master_name
                ,b."Char2" as "CheckStep"
                from bundle_head b
                inner join check_result cr on cr."SourceDataPk" = b.id and cr."SourceTableName" = 'bundle_head'
                and cr."CheckMaster_id" = (select id from check_mast where "Name" = '에어컴프레샤')
                inner join user_profile cu on b._creater_id = cu."User_id"
                left join user_profile uu on b._modifier_id = uu."User_id"
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                left join check_mast cm on cm.id = cr."CheckMaster_id"
                where b."TableName" = 'check_result_cross_contamination1'
                and b.id = :bhId                    
            """;   
			head_info = this.sqlRunner.getRow(sql, paramMap);
			
			
			 sql = """
	                    select t.id
		                    , r1."CheckerName" as "CheckName1", coalesce(to_char(r1."CheckDate", 'yyyy-MM-dd'), '-') as "CheckDate1", r1."Char1" ApprName1, CAST(r1."Char2" AS text) as "ApprDate1"
		                    , r2."CheckerName" as "CheckName2", coalesce(to_char(r2."CheckDate", 'yyyy-MM-dd'), '-') as "CheckDate2", r2."Char1" ApprName2, CAST(r2."Char2" AS text) as "ApprDate2"
		                    , r3."CheckerName" as "CheckName3", coalesce(to_char(r3."CheckDate", 'yyyy-MM-dd'), '-') as "CheckDate3", r3."Char1" ApprName3, CAST(r3."Char2" AS text) as "ApprDate3"
		                    , r4."CheckerName" as "CheckName4", coalesce(to_char(r4."CheckDate", 'yyyy-MM-dd'), '-') as "CheckDate4", r4."Char1" ApprName4, CAST(r4."Char2" AS text) as "ApprDate4"
		                    , r5."CheckerName" as "CheckName5", coalesce(to_char(r5."CheckDate", 'yyyy-MM-dd'), '-') as "CheckDate5", r5."Char1" ApprName5, CAST(r5."Char2" AS text) as "ApprDate5"
	                    from bundle_head t
	                    left join check_result r1 on t.id = r1."SourceDataPk" and r1."SourceTableName" = 'bundle_head' and r1."Number2" = 1
	                    left join check_result r2 on t.id = r2."SourceDataPk" and r2."SourceTableName" = 'bundle_head' and r2."Number2" = 2
	                    left join check_result r3 on t.id = r3."SourceDataPk" and r3."SourceTableName" = 'bundle_head' and r3."Number2" = 3
	                    left join check_result r4 on t.id = r4."SourceDataPk" and r4."SourceTableName" = 'bundle_head' and r4."Number2" = 4
	                    left join check_result r5 on t.id = r5."SourceDataPk" and r5."SourceTableName" = 'bundle_head' and r5."Number2" = 5
	                    where t.id = :bhId
			
			 		""";
			 diary_grp_info = this.sqlRunner.getRow(sql, paramMap);
			
		}else {
			head_info.put("id", 0);
			head_info.put("Title", "에어컴프레샤 정기점검일지");
			head_info.put("DataDate", data_Date);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("StateName", "상신대기");
			head_info.put("check_result_id", 0);
			head_info.put("Description", "");
			head_info.put("CheckStep", 1);
			
			
			diary_grp_info.put("id",0);
			diary_grp_info.put("CheckName1","");
			diary_grp_info.put("CheckDate1","-");
			diary_grp_info.put("ApprName1","");
			diary_grp_info.put("ApprDate1","");
			diary_grp_info.put("CheckName2","");
			diary_grp_info.put("CheckDate2","-");
			diary_grp_info.put("ApprName2","");
			diary_grp_info.put("ApprDate2","");
			diary_grp_info.put("CheckName3","");
			diary_grp_info.put("CheckDate3","-");
			diary_grp_info.put("ApprName3","");
			diary_grp_info.put("ApprDate3","");
			diary_grp_info.put("CheckName4","");
			diary_grp_info.put("CheckDate4","-");
			diary_grp_info.put("ApprName4","");
			diary_grp_info.put("ApprDate4","");
			diary_grp_info.put("CheckName5","");
			diary_grp_info.put("CheckDate5","-");
			diary_grp_info.put("ApprName5","");
			diary_grp_info.put("ApprDate5","");
			
			Integer checkMasterId = 0;
			List<CheckMaster> cm1 = this.checkMasterRepository.findByName("에어컴프레샤");
			if (cm1.size() > 0) {
				checkMasterId = cm1.get(0).getId();
			}
			
			head_info.put("CheckMaster_id", checkMasterId);
			head_info.put("CheckCycle", cm1.get(0).getCheckCycle());			
		}
			sql = """
					with r as (
	                select rr.id, rr."CheckItem_id", rr."CheckResult_id", rr."Result1", rr."Result2", rr."Result3", rr._order, r."Number2" as CheckStep
	                from check_result r
	                inner join check_item_result rr on r.id = rr."CheckResult_id"
	                where r."SourceDataPk" = :bhId
	                and r."SourceTableName" = 'bundle_head'
                )
                select max(r.id) as id, ci.id as item_id, ci."ItemGroup1" as group1, ci."ItemGroup2" as group2, ci."ItemGroup3" as group3, ci."Name" as item_name
                  , max(case when r."Result3"='1' then r."Result1" else null end) as result1
                 , max(case when r."Result3"='2' then r."Result1" else null end) as result2
                 , max(case when r."Result3"='3' then r."Result1" else null end) as result3
                 , max(case when r."Result3"='4' then r."Result1" else null end) as result4
                 , max(case when r."Result3"='5' then r."Result1" else null end) as result5
                 ,max(case when r."Result3"='1' then r."Result2" else null end) as resultDate1
                 ,max(case when r."Result3"='2' then r."Result2" else null end) as resultDate2
                  ,max(case when r."Result3"='3' then r."Result2" else null end) as resultDate3
                 ,max(case when r."Result3"='4' then r."Result2" else null end) as resultDate4
                  ,max(case when r."Result3"='5' then r."Result2" else null end) as resultDate5
                  , max(r."Result3") as type
                , max(r."CheckItem_id") as checkItem_id, max(r."CheckResult_id") as checkResult_id , max(r._order) as index_order, max(af.id) as file_id, ci._order
                from check_item ci
                left join r on r."CheckItem_id" = ci.id 
                left join attach_file af on af."DataPk" = ci.id and "TableName" = 'check_item'
                where ci."CheckMaster_id" = (select id from check_mast where "Name" = '에어컴프레샤')
                  group by item_id
                order by ci._order
				""";
			diary_info = this.sqlRunner.getRows(sql,paramMap);
			
			sql = """
					 select da.id	                
                    , da."HappenDate" as happen_date
	                , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
	                , da."ActionState" as action_state, da."ConfirmState" as confirm_state
	                , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name	                
	                , up2."Name" as creater_name
	                , RIGHT(da."AbnormalDetail", POSITION(':' IN REVERSE(da."AbnormalDetail"))-1) as check_name
	                , cir._order as _order	
	                , ci."ItemGroup1" as happen_place
	                , da."ConfirmDetail" as confirm_detail
	                , da."Actor_id" as actor_id
	                , da."ActorName" as actor_name
	                , da."Confer_id" as confer_id
	                , da."ConferName" as confer_name
                    , to_char(da._modified,'yyyy-MM-dd') as actor_date
                from v_devi_action da
                inner join check_item_result cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'devi_action_cross_contamination1'
                inner join check_item ci on ci.id = cir."CheckItem_id"
                inner join check_result cr on cr.id = cir."CheckResult_id"  and cr."SourceDataPk" = :bhId
                left join user_profile up on up."User_id" = da._modifier_id
                left join user_profile up2 on up2."User_id" = da._creater_id
                order by cr.id
					""";
			
			action_info = this.sqlRunner.getRows(sql, paramMap);
			
		
		
		items.put("action_info", action_info);
		items.put("diary_info", diary_info);
		items.put("head_info", head_info);
		items.put("diary_grp_info", diary_grp_info);
		
		
		return items;
	}

	public void mstDelete(Integer bhId) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		 sql = """
				delete from check_item_result                     
         where "CheckResult_id" in (
             select id from check_result 
             where "SourceDataPk" = :bhId	and "SourceTableName" = 'bundle_head'                
         )
         and "CheckItem_id" in (
             select distinct ci.id 
             from check_result cr 
             inner join check_item  ci
             on cr."CheckMaster_id" = ci."CheckMaster_id"
             where cr."SourceTableName" = 'bundle_head' 
             and cr."SourceDataPk" = :bhId
         )
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		 sql = """
				delete from check_result 
                where "SourceDataPk" = :bhId
                and "SourceTableName" = 'bundle_head'
				""";
		 this.sqlRunner.execute(sql, paramMap); 
		 
		 
		 
		 sql = """
		 		delete from devi_action
                where "SourceDataPk" not in (
	                select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and "Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1		               
                )
                and "SourceTableName" = 'devi_action_cross_contamination1'
		 		""";
		 this.sqlRunner.execute(sql, paramMap); 
		 
		 
		 sql = """
		 		 delete from bundle_head where id = :bhId
		 		""";
		 
		 this.sqlRunner.execute(sql, paramMap); 
		
	}

}
