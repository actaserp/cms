package mes.app.haccp.verification_result.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class VertiResultReportService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getVertiResultReport(Integer checkMasterId, String startDate, String endDate,
			String apprStatus) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("checkMasterId", checkMasterId);
		paramMap.addValue("apprStatus", apprStatus);
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
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
				where b."TableName" = 'verti_result_report'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
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

	public Map<String, Object> getResultList(Integer bhId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		
		String sql = null;
		
		sql = """
				select b.id, b."Char1" as title, "Number1" as check_master_id, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", cm."CheckCycle" 
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                left join check_mast cm on cm.id = b."Number1"
				where b."TableName" = 'verti_result_report'
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
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		item.put("mst_info", mstInfo);
		item.put("week_result", weekResult);
		
		return item;
	}

	@Transactional
	public void reportDelete(Integer bhId) {
		
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
