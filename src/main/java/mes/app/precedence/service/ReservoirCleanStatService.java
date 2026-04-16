package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.entity.CheckMaster;
import mes.domain.entity.User;
import mes.domain.repository.CheckMasterRepository;
import mes.domain.services.SqlRunner;

@Service
public class ReservoirCleanStatService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;
	
	public List<Map<String, Object>> getApprStat(String startDate, String endDate, String apprStatus) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("apprStatus", apprStatus);
		
		String sql = """
					 select b.id, b."Char1" as "Title", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '상신대기') as "StateName"
					 , r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM') as "DataDate", coalesce(r."SearchYN", 'Y') as "SearchYN"
					 , coalesce(r."EditYN", 'Y') as "EditYN", coalesce(r."DeleteYN", 'Y') as "DeleteYN"
					 , b."Number1" as check_master_id, b._creater_id ,up."Name" as creater_name , b._modifier_id
					 , up2."Name" as modifier_name, cm."CheckCycle" , b."Text1"
					from bundle_head b                               
					left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					left join user_profile up on b._creater_id = up."User_id"
					left join user_profile up2 on b._modifier_id = up2."User_id"
					left join check_mast cm on cm.id = b."Number1"
					where b."TableName" = 'check_result_reservoir_clean'
					and b."Date1" between cast(:startDate as date) and cast(:endDate as date)  
				""";
		
		if (StringUtils.hasText(apprStatus)) {
            if (apprStatus.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprStatus ";
            }
		}
		
		sql += " order by b.\"Date1\" desc, b.id desc ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
		return items;
	}

	public Map<String, Object> getReservoirClean(Integer bhId, String dataDate, String endDate, User user) {
		
		Map<String,Object> headInfo = new HashMap<>();
		
		Optional<CheckMaster> cm = this.checkMasterRepository.findByCode("저수조용수관리점검표");
		
		String sql = null;
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		if(bhId > 0) {
			sql = """
					select b.id, b."Char1" as "Title", to_char(b."Date1", 'yyyy-MM') as "DataDate"
					, coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State"
					, coalesce(r."StateName", '상신대기') as "StateName"  , b."Text1"
					, cr.id as check_result_id ,cr."CheckMaster_id", cr."Description", cm."Name" as check_master_name
					from bundle_head b
					inner join check_result cr on cr."SourceDataPk" = b.id
					and cr."CheckMaster_id" = (select id from check_mast where "Name" = '저수조용수관리점검표')
					inner join user_profile cu on b._creater_id = cu."User_id"
					left join user_profile uu on b._modifier_id = uu."User_id"
					left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					left join check_mast cm on cm.id = cr."CheckMaster_id"
					where b."TableName" = 'check_result_reservoir_clean'
					and b.id = :bhId 
				  """;
			
			headInfo = this.sqlRunner.getRow(sql, paramMap);
		} else {
			headInfo.put("id", 0);
			headInfo.put("Title", "저수조용수 관리점검표");
			headInfo.put("DataDate", dataDate);
			headInfo.put("FirstName", user.getUserProfile().getName());
			headInfo.put("StateName", "상신대기");
			headInfo.put("check_result_id", 0);
			headInfo.put("Description", "");
		}
		headInfo.put("CheckMaster_id", cm.get().getId());
		headInfo.put("CheckCycle", cm.get().getCheckCycle());
		
		sql = """
				select r.id, ci.id as item_id, ci."ItemGroup1" as group1, ci."ItemGroup2" as group2
				, ci."ItemGroup3" as group3, ci."Name" as item_name, "Result1" as result1, "Result2" as result2, "CheckItem_id"
				, "CheckResult_id" , r._order as index_order, af.id as file_id ,ci._order
				from check_item ci
				left join check_item_result r on r."CheckItem_id" = ci.id 
				and  "CheckResult_id" in(
				    select id from check_result where "SourceDataPk" = :bhId 
				    )
				left join attach_file af on af."DataPk" = ci.id and "TableName" = 'check_item'
				where "CheckMaster_id" = (select id from check_mast where "Name" = '저수조용수관리점검표')
				order by ci._order
			  """;
		
		List<Map<String, Object>> diaryInfo = this.sqlRunner.getRows(sql, paramMap);
		
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
			inner join check_item_result cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'devi_action_reservoir_clean'
			inner join check_item ci on ci.id = cir."CheckItem_id"
			inner join check_result cr on cr.id = cir."CheckResult_id"  and cr."SourceDataPk" = :bhId
			left join user_profile up on up."User_id" = da._modifier_id
			left join user_profile up2 on up2."User_id" = da._creater_id
			order by cr.id
			  """;
		
		List<Map<String, Object>> actionInfo = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> items = new HashMap<>();
		items.put("head_info", headInfo);
		items.put("diary_info", diaryInfo);
		items.put("action_info", actionInfo);
		
		return items;
	}

	@Transactional
	public void mstDelete(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = """
                delete from check_item_result                     
                where "CheckResult_id" in (
	                select id from check_result 
	                where "SourceDataPk" = :bhId
	                and "Char1" = 'reservior_clean'                
                )
                and "CheckItem_id" in (
	                select distinct ci.id 
	                from check_result cr 
	                inner join check_item  ci on cr."CheckMaster_id" = ci."CheckMaster_id"
	                where cr."Char1" = 'reservior_clean' 
                    and cr."SourceDataPk" = :bhId
                )   
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
                delete from check_result 
                where "SourceDataPk" = :bhId
                and "Char1" = 'reservior_clean'
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
                and "SourceTableName" = 'devi_action_reservoir_clean'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
	}

}
