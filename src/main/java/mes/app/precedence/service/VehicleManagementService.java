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
public class VehicleManagementService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	CheckMasterRepository checkMasterRepository;

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
				where b."TableName" = 'vehicle_management'
				and b."Date1" between cast(:start_date as date) and cast(:end_date as date)
				order by b."Date1" desc
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		
		return items;
	}

	
	
	
	
	
	public Map<String, Object> apprList(Integer bhId, String data_date,Authentication auth) {

		User user = (User)auth.getPrincipal();
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("data_date", data_date);
		paramMap.addValue("bhId", bhId);	
		
		String titleDate = data_date;
		
		
		Map<String, Object> head_info = new HashMap<>();
		
		String sql = null;
		if(bhId > 0) {
			sql = """
					select bh.id
                , bh."Char1" as "Title"
                , bh."Char2" as "Description"
                , to_char(bh."Date1", 'yyyy-MM-DD') as "DataDate"
                , coalesce(uu."Name", cu."Name") as "FirstName"
                , coalesce(r."State", 'write') as "State"
                , coalesce(r."StateName", '작성') as "StateName"
                from bundle_head bh
                inner join user_profile cu on bh._creater_id = cu."User_id"
                left join user_profile uu on bh._modifier_id = uu."User_id"
                left join v_appr_result r on bh.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where bh."TableName" = 'vehicle_management'
                 and bh.id = :bhId
					""";
			
			head_info = this.sqlRunner.getRow(sql, paramMap);
		}else {
			head_info.put("id", 0);
			head_info.put("Title", "출고차량점검표");
			head_info.put("DataDate", titleDate);
			head_info.put("FirstName", user.getUserProfile().getName());
			head_info.put("StateName", "작성");
			head_info.put("check_result_id", 0);
			head_info.put("Description", "");
			
			
			Integer checkMasterId = 0;
			List<CheckMaster> cm1 = this.checkMasterRepository.findByName("출고차량점검표");
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
	                --and r."SourceTableName" = 'bundle_head'
                )
                select max(r.id) as id, ci.id as item_id, ci."ItemGroup1" as group1, ci."ItemGroup2" as group2, ci."ItemGroup3" as group3, ci."Name" as item_name
               , max("Result1") as result1
               ,ci."ResultType" as result_type
                , max(r."CheckItem_id") as checkItem_id, max(r."CheckResult_id") as checkResult_id , max(r._order) as index_order, max(af.id) as file_id, ci._order
                , aff.id as "imageFileId"
                from check_item ci
                left join r on r."CheckItem_id" = ci.id 
                left join attach_file af on af."DataPk" = ci.id and "TableName" = 'check_item'
                left join attach_file aff on aff."TableName"  = 'vehicle_manage_grid' and aff."DataPk" = r.id 
                where ci."CheckMaster_id" = (select id from check_mast where "Name" = '출고차량점검표')
                  group by item_id, aff.id
                order by ci._order
                
				""";
		 
		 List<Map<String, Object>> diary_info = this.sqlRunner.getRows(sql, paramMap);
		 
		 sql = """
	                select da.id
			            , da."HappenDate" as happen_date, da."HappenPlace" as happen_place
			            , da."AbnormalDetail" as abnormal_detail, da."ActionDetail" as action_detail
			            , da."ActionState" as action_state, da."ConfirmState" as confirm_state
			            , da."SourceDataPk" as src_data_pk, da."SourceTableName" as src_table_name
			            , up."Name" as actor_name
			            , up2."Name" as creater_name
			            , RIGHT(da."AbnormalDetail", POSITION(':' in REVERSE(da."AbnormalDetail"))-1) as check_name
			            , cir._order as _order
		            from devi_action da
		            inner join check_item_result cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'check_item_devi_result'
		            inner join check_result cr on cr.id = cir."CheckResult_id" 
	                and cr."SourceDataPk" = :bhId 
		            left join user_profile up on up."User_id" = da._modifier_id
		            left join user_profile up2 on up2."User_id" = da._creater_id
	                order by cr.id			
				  """;
			
		List<Map<String, Object>> action_info = this.sqlRunner.getRows(sql, paramMap);
		 
		 
		 
		 Map<String, Object> items = new HashMap<>();
		 items.put("head_info", head_info);
		 items.put("diary_info", diary_info);
		 items.put("action_info", action_info);
		
		return items;
	}






	public Map<String, Object> delete(Integer bhId) {
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
            and cr."SourceDataPk" = :bhId
        )
				""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from check_result 
                where "SourceDataPk" = :bhId
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
			 		""";
			 this.sqlRunner.execute(sql, paramMap);
		
		 sql = """
		 		 delete from bundle_head where id = :bhId
		 		""";
		 
		 this.sqlRunner.execute(sql, paramMap); 
		
		
		return null;
	}

}
