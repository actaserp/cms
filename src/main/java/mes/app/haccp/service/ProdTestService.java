package mes.app.haccp.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class ProdTestService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getReadList(String startDate, String endDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		
		String sql = """
				select b.id, b."Char1" as "Title", coalesce(r."StateName", '작성') as "StateName"
				, r."LineName", r."LineNameState", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, coalesce(r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
				, coalesce(r."DeleteYN", 'Y') as "DeleteYN", b."Number1" as check_master_id
				, b._creater_id ,up."Name" as "creater_name" , b._modifier_id, up2."Name" as "modifier_name"
				from bundle_head b                               
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				left join user_profile up on b._creater_id = up."User_id"
				left join user_profile up2 on b._modifier_id = up2."User_id"
				where b."TableName" = 'prod_test_result'
				and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		sql += " order by b.\"Date1\" desc ";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		
		return items;
	}

	public Map<String, Object> getResultList(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
				select b.id, b."Char1" as title, to_char(b."Date1", 'yyyy-MM-dd') as "DataDate"
				, b."Char2" as "startDt", b."Char3" as "endDt"
                from bundle_head b
				inner join user_profile cu on b._creater_id = cu."User_id"
				left join user_profile uu on b._modifier_id = uu."User_id"
				left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
				where b."TableName" = 'prod_test_result'
                and b.id = :bhId
				""";
		
		Map<String,Object> mstInfo = this.sqlRunner.getRow(sql, paramMap);
		
		sql = """
				select ti.id, ti."Name" 
				from test_item_mast tim 
				inner join test_mast tm on tim."TestMaster_id"  = tm.id 
				inner join test_item ti on tim."TestItem_id"  = ti.id
				where tm."Name"  = '제품검사'
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);
		
		
		sql = """
				  select jr.id, jr."ProductionDate" as "productionDate", m."Name" as name 
				  , jr."WorkOrderNumber" as "workOrderNumber"
				  """;
			
			if (items.size() > 0) {
				for (int i = 0; i < items.size(); i++) {
					sql += ", max(case when ti.id = " + items.get(i).get("id") + " then tir.\"Char2\" end) as \"" + items.get(i).get("Name") + "\"";
				}
			}
			
			sql += """
					, tir."JudgeCode" as "judgeCode"
					from test_item_mast tim
					inner join test_mast tm on tim."TestMaster_id"  = tm.id 
					inner join test_item ti on tim."TestItem_id"  = ti.id
					inner join test_item_result tir on tir."TestItem_id"  = ti.id 
					inner join test_result tr on tr.id = tir."TestResult_id" 
					inner join job_res jr on jr.id = tr."SourceDataPk" and tr."SourceTableName" = 'job_res'
					inner join rela_data rd on jr.id = rd."DataPk2"
					inner join material m on m.id = jr."Material_id"
					where 1 = 1
					and rd."DataPk1"  = :bhId
					and rd."RelationName"  = 'prod_test_result'
					group by jr.id, jr."ProductionDate", m."Name", jr."WorkOrderNumber", tir."JudgeCode"
					order by jr."WorkOrderNumber"
					""";
			
			List<Map<String, Object>> itemResult = this.sqlRunner.getRows(sql, paramMap);
			
			Map<String,Object> item = new HashMap<>();
			
			item.put("mst_info", mstInfo);
			item.put("item_result", itemResult);
			
			return item;
	}

	public Map<String, Object> getResultListDefault(String srchStartDt, String srchEndDt) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("srchStartDt", srchStartDt);
		paramMap.addValue("srchEndDt", srchEndDt);
		
		String sql = """
				select ti.id, ti."Name" 
				from test_item_mast tim 
				inner join test_mast tm on tim."TestMaster_id"  = tm.id 
				inner join test_item ti on tim."TestItem_id"  = ti.id
				where tm."Name"  = '제품검사'
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);
		
		
		sql = """
			  select jr.id, jr."ProductionDate" as "productionDate", m."Name" as name 
			  , jr."WorkOrderNumber" as "workOrderNumber"
			  """;
		
		if (items.size() > 0) {
			for (int i = 0; i < items.size(); i++) {
				sql += ", max(case when ti.id = " + items.get(i).get("id") + " then tir.\"Char2\" end) as \"" + items.get(i).get("Name") + "\"";
			}
		}
		
		sql += """
				, tir."JudgeCode" as "judgeCode"
				from test_item_mast tim
				inner join test_mast tm on tim."TestMaster_id"  = tm.id 
				inner join test_item ti on tim."TestItem_id"  = ti.id
				inner join test_item_result tir on tir."TestItem_id"  = ti.id 
				inner join test_result tr on tr.id = tir."TestResult_id" 
				inner join job_res jr on jr.id = tr."SourceDataPk" and tr."SourceTableName" = 'job_res'
				inner join material m on m.id = jr."Material_id"
				where 1 = 1
				and jr."ProductionDate" between cast(:srchStartDt as date) and cast(:srchEndDt as date)
				group by jr.id, jr."ProductionDate", m."Name", jr."WorkOrderNumber", tir."JudgeCode"
				order by jr."WorkOrderNumber"
				""";
		
		List<Map<String, Object>> itemResult = this.sqlRunner.getRows(sql, paramMap);
		
		Map<String,Object> item = new HashMap<>();
		
		item.put("item_result", itemResult);
		
		return item;
	}

	@Transactional
	public void mstDelete(Integer bhId) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		
		String sql = null;
		
		sql = """
            delete from rela_data                     
            where "DataPk1" = :bhId
            and "RelationName" = 'prod_test_result'
            and "TableName1"  = 'bundle_head'
			and "TableName2"  = 'job_res'
			""";
		
		this.sqlRunner.execute(sql, paramMap);
		
		sql = """
				delete from bundle_head where id = :bhId
			  """;
			
		this.sqlRunner.execute(sql, paramMap);
		
	}

}
