package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class SanitationCheckResultService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getSanitationCheck(Integer checkMasterId, String startDate, String endDate,
			String apprStatus, String type) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("checkMasterId", checkMasterId);
		paramMap.addValue("apprStatus", apprStatus);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("type", type);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, cm."Code" as "checkCode"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				, cm."CheckCycle" 
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'check_result_sanitation_check_result'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (StringUtils.hasText(type)) {
			sql += " and cm.\"Code\" != '위생시설점검일지(일반해썹)' ";
		}
		
		if (checkMasterId != null) {
			sql += " and b.\"Number1\" = :checkMasterId ";
		}
		
		if (!apprStatus.isEmpty() && apprStatus != null) {
            if (apprStatus.equals("write")) {
            	sql += " and r.\"State\" is null ";
            } else {
            	sql += " and r.\"State\" = :apprStatus ";
            }
		}
		
		sql += " order by b.\"Date1\" desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getResultList(Integer bhId, Integer checkMasterId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("checkMasterId", checkMasterId);
		
		
		String sql = null;
		
		sql = """
				select b.id, b."Char1" as title, cm.id as check_master_id, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", cm."CheckCycle" 
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'check_result_sanitation_check_result'
                and b.id = :bhId
				""";
		
		Map<String,Object> mstInfo = this.sqlRunner.getRow(sql, paramMap);
		
		sql = """
			  select id, "CheckDate" as checkdate , "SourceDataPk" as sourcedatapk,"Description" as description
              , "CheckMaster_id" as checkMaster_id, "Char1" as char1, "CheckerName" 
              from check_result 
              where "SourceDataPk" = :bhId
			  """;
		
		Map<String,Object> weekResult = this.sqlRunner.getRow(sql, paramMap);
		
		sql = """
			    select r.id, ir."ItemGroup1" as group1, ir."ItemGroup2" as group2, ir."ItemGroup3" as group3, ir."Name" as item_name
                , ir."ResultType" as result_type, "Result1" as result1, "Result2" as result2, "CheckItem_id", "CheckResult_id" , r._order as index_order, af.id as file_id
                from check_item_result r
                inner join check_item ir on r."CheckItem_id" = ir.id
                left join attach_file af on af."DataPk" = ir.id and "TableName" = 'check_item'
                where "CheckResult_id" in(
                select id from check_result where "SourceDataPk" = :bhId)
                order by r._order 
			  """;
		
		List<Map<String, Object>> itemResult = this.sqlRunner.getRows(sql, paramMap);
		
		weekResult.put("item_result", itemResult);
		
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
		
		List<Map<String, Object>> itemDeviResult = this.sqlRunner.getRows(sql, paramMap);
		
		weekResult.put("item_devi_result", itemDeviResult);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("mst_info", mstInfo);
		item.put("week_result", weekResult);
		
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
                and "SourceTableName" = 'check_item_devi_result'
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
			   delete from bundle_head where id = :bhId
			  """;
		
		this.sqlRunner.execute(sql, paramMap);
	}

	public List<Map<String, Object>> getCheckItem(Integer checkMasterId, String checkDate, String startDate, String endDate) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("checkMasterId", checkMasterId);  
		paramMap.addValue("checkDate", checkDate);  
		paramMap.addValue("startDate", startDate);  
		paramMap.addValue("endDate", endDate);  
		
		String sql = """
				select ci.id
	            , c."Name" as check_master_name
	            , ci."ItemGroup1" as group1
	            , ci."ItemGroup2" as group2
	            , ci."ItemGroup3" as group3
	            , ci."Name" as item_name
	            , ci."Code" as item_code 
	            , ci."CycleType" as cycle_type 
	            , ci."CycleValue" as cycle_value
	            , ci."ResultType" as result_type
	            , ci."StartDate" as start_date
	            , ci."EndDate" as end_date
	            , ci."_order" as index_order
	            , ci."minValue" as min_value
	            , ci."maxValue" as max_value
	            from check_item ci
	            inner join check_mast c on c.id = ci."CheckMaster_id"
	            where 1=1
				""";
		
		if (checkMasterId != null) sql += " and c.\"id\" = :checkMasterId";
		if(checkDate != null && !checkDate.isEmpty()) {
			sql += " and ci.\"StartDate\" <= cast(:checkDate as date) and ci.\"EndDate\" >= cast(:checkDate as date) ";
		} else {
			sql += " and ci.\"StartDate\" <= cast(:startDate as date) and ci.\"EndDate\" >= cast(:endDate as date) ";
		}
		
		sql += " order by ci._order asc, C.\"Name\" desc ";
				
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

}
