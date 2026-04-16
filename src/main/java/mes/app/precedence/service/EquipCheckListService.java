package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class EquipCheckListService {

	@Autowired
	SqlRunner sqlRunner;
	
	public Map<String, Object> getApprStat(Integer checkMasterId, String startDate, String endDate,
			String apprState) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("checkMasterId", checkMasterId);
		paramMap.addValue("apprState", apprState);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		
		String sql = """
				select b.id, b."Char1" as "Title"
                   , coalesce(r."StateName", '작성') as "StateName"
                   , r."LineName"
                   , r."LineNameState"
                   , cm."Code"
                   , b."Char2" as "StartDate"
                   , b."Char3" as "EndDate"
                   , b."Char2" as "CheckMasterName"
                   , coalesce(r."SearchYN", cast('Y' as text)) as "SearchYN"
                   , coalesce(r."EditYN", 'Y') as "EditYN"
                   , coalesce(r."DeleteYN", 'Y') as "DeleteYN"             
                from bundle_head b
                inner join check_result cr on b.id = cr."SourceDataPk" 
                inner join check_mast cm on cr."CheckMaster_id" = cm.id
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where b."TableName" = 'prod_equip_check_list'
                and (b."Char2" >= :startDate and b."Char3" <= :endDate)
				""";
		
		if (checkMasterId != null) {
			sql += " and cr.\"CheckMaster_id\" = :checkMasterId ";
		}
		
		if (!apprState.isEmpty() && apprState != null) {
            if (apprState.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprState ";
            }
		}
		
		sql += " order by b.\"Date1\" desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("document_info", items);
		
		return item;
	}

	public Map<String, Object> getEquipCheckList(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		Map<String, Object> item = new HashMap<String, Object>();
		if (bhId > 0) {
			
		 String sql = """
					select b.id, b."Char1" as "Title", b."Char2" as "StartDate", b."Char3" as "EndDate", b."Text1" as "CheckMasterId"
                    , coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '작성') as "StateName"
                    from bundle_head b
					inner join user_profile cu on b._creater_id = cu."User_id"
					left join user_profile uu on b._modifier_id = uu."User_id"
					left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					where b."TableName" = 'prod_equip_check_list'
                    and b.id = :bhId
		 			""";
		 
		Map<String, Object> item1 = this.sqlRunner.getRow(sql, paramMap);
			 
		String sql2 = """
				 		select ir."CheckItem_id" as id
		                , ci."ItemGroup1" as group1
		                , ci."ItemGroup2" as group2
		                , ci."ItemGroup3" as group3
		                , ci."Name" as item_name
		                , ci."ResultType" as result_type
		                , ir."Result1" as result1
		                , ir."Result2" as result2
		                , ir."Result3" as result3
		                , ir._order as "_order"
		                , ir."CheckResult_id"
		                from check_item_result ir
		                inner join check_item ci on ci.id = ir."CheckItem_id"
		                left join check_result cr on cr.id = ir."CheckResult_id"
		                where cr."SourceDataPk" = :bhId
		                order by ir._order
		 			   """;
			
		List<Map<String, Object>> item2 = this.sqlRunner.getRows(sql2, paramMap);
		
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
	            inner join check_item_result cir on cir.id = da."SourceDataPk" and da."SourceTableName" = 'prod_equip_check_list'
	            inner join check_result cr on cr.id = cir."CheckResult_id" 
                and cr."SourceDataPk" = :bhId
	            left join user_profile up on up."User_id" = da._modifier_id
	            left join user_profile up2 on up2."User_id" = da._creater_id
                order by cr.id			
			  """;
		
		List<Map<String, Object>> itemDeviResult = this.sqlRunner.getRows(sql, paramMap);
		
		
		item.put("head_info", item1);
		item.put("diary_info", item2);
		item.put("item_devi_result", itemDeviResult);
		
		}
	
		return item;
	}

	@Transactional
	public void mstDelete(Integer bhId) {
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
              delete from devi_action
                where "SourceDataPk" not in (
	                select b.id
	                from check_result a
	                inner join check_item_result b on a.id = b."CheckResult_id" and "Result1" = 'X'
	                left join  devi_action c on c."SourceDataPk" = b.id
	                where 1=1		               
                )
                and "SourceTableName" = 'prod_equip_check_list'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			   delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
	}

}
