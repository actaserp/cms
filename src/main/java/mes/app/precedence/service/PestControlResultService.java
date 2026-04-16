package mes.app.precedence.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.entity.User;
import mes.domain.services.SqlRunner;

@Service
public class PestControlResultService {

	@Autowired
	SqlRunner sqlRunner;
	
//	public List<Map<String, Object>> getPestControlResult(String docForm, String dateFrom, String dateTo, Integer id) {
//		
//		MapSqlParameterSource paramMap = new MapSqlParameterSource();
//		paramMap.addValue("docForm", docForm);
//		paramMap.addValue("dateFrom", dateFrom);
//		paramMap.addValue("dateTo", dateTo);
//		paramMap.addValue("id", id);
//		
//		String sql = """
//				with A as (
//	            select dr.id 
//	            , f."FormName" as form_name
//	            , to_char(dr."DocumentDate",'yyyy-mm-dd') as doc_date
//				, dr."DocumentName" as doc_name
//	            , dr."Content" as check_result
//	            , dr."Description" as pest_result
//	            , dr."Text1" as problem
//	            , dr."Text2" as action
//	            from doc_result dr 
//	            inner join doc_form f on f.id = dr."DocumentForm_id"
//	            where 1=1
//	            """;
//				if(id != null) {
//					sql += " and dr.\"DocumentForm_id\" = :id ";
//				}
//				
//	  sql +=    """
//	            and dr."DocumentDate" between cast(:dateFrom as date) and cast(:dateTo as date)
//		        ), B as (
//	            select rd."DataPk1" as data_pk1, sum(rd."Number1") as pest_count
//	            , sum(case when p."Type" = 'flight' then rd."Number1" end) as flight_count
//	            , sum(case when p."Type" = 'walk' then rd."Number1" end) as walk_count
//	            , sum(case when p."Type" = 'rodent' then rd."Number1" end) as rodent_count
//	            from rela3_data rd 
//	            inner join A on A.id = rd."DataPk1"
//	            and rd."TableName1" = 'doc_result'
//	            inner join master_t p on p.id = rd."DataPk3"
//	            and p."MasterClass" = 'pest'
//	            where rd."RelationName" = 'pest_control_detail'
//	            group by rd."DataPk1"
//	            )
//	            select A.*, B.*
//	            from A 
//	            left join B on B.data_pk1 = A.id
//	            order by A.doc_date
//				""";
//		
//        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
//
//        return items;
//	}

	public Map<String, Object> getPestControlResultDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select dr.id as id
                , f."FormName" as form_name
                , f.id as doc_form_id
	            , to_char(dr."DocumentDate",'yyyy-mm-dd') as doc_date
	            , dr."Content" as check_result
	            , dr."Description" as pest_result
	            , dr."Text1" as problem
	            , dr."Text2" as action
	            from doc_result dr 
	            inner join doc_form f on f.id = dr."DocumentForm_id"
		        where 1=1 
                and dr.id= :id
				""";
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
	}

	public Map<String,Object> getPestCountSheet(Integer id) {
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("id", id);
		
		String sql = """
				select p.id as pest_id, p."Name" as pest_name, p."Code" as pest_code, p."Type" as pest_class_code
	            , p._order
	            from master_t p
	            where p."MasterClass" = 'pest'
	            order by case p."Type" when 'flight' then 1 when 'walk' then 2 else 3 end, p._order, p."Name"
				""";
		
        List<Map<String, Object>> pestList = this.sqlRunner.getRows(sql, paramMap);
        
        String sql2 = " select rd.\"DataPk1\" as data_pk1, t.id as trap_id, t.\"Name\" as trap_name ";
        for (int i = 0; i < pestList.size(); i++) {
        	sql2 += " , min(case when p.id = " + pestList.get(i).get("pest_id") + " then rd.\"Number1\" end) as pest_" + (i+1) + " ";
        }
        sql2 += """
	    		from master_t t 
	            left join  rela3_data rd  on t.id = rd."DataPk2"
	            and rd."DataPk1" = :id
	            and rd."TableName1" = 'doc_result'
	            and rd."RelationName" = 'pest_control_detail'
	            left join master_t p on p.id = rd."DataPk3"
	            and p."MasterClass" = 'pest'
		        where t."MasterClass" = 'pest_trap'
	            group by rd."DataPk1" , t.id , t."Name"
	            order by t."Name"
    		  """;
        
        List<Map<String, Object>> pestCountSheet = this.sqlRunner.getRows(sql2, paramMap);
        
        Map<String,Object> test = new HashMap<>();
        
        test.put("pest_list", pestList);
        test.put("pest_count_sheet", pestCountSheet);
        
        return test;
	}

	public Map<String, Object> getApprStat(String startDate, String endDate, String pestTrapClass) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("startDate", startDate);
		paramMap.addValue("endDate", endDate);
		paramMap.addValue("pestTrapClass", pestTrapClass);
		
		
		String sql = """
                with c as (
	                select uc.id, uc."Code", uc."Value", uc._order
	                from user_code uc
	                where exists (
		                select 1 from user_code where "Code" = 'pest_trap_class' and uc."Parent_id" = id
	                )
                )
                select b.id, b."Char1" as "Title", c."Code" as "PestTrapClass", c."Value" as "PestTrapClassName"
					, coalesce(r."StateName", '작성') as "StateName", r."LineName", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", coalesce (r."SearchYN", 'Y') as "SearchYN", coalesce(r."EditYN", 'Y') as "EditYN"
					, coalesce (r."DeleteYN", 'Y') as "DeleteYN"
                from bundle_head b
				inner join c on b."Char2" = c."Code"
                left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
                where b."TableName" = 'check_result_pest_control_result'
                and b."Date1" between cast(:startDate as date) and cast(:endDate as date)
				""";
		
		if (!pestTrapClass.isEmpty()) {
			sql += " and b.\"Char2\" = :pestTrapClass ";
		}
		
		sql += " order by b.\"Date1\" desc ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        Map<String, Object> docInfo = new HashMap<String, Object>();
        docInfo.put("document_info", items);
        
        return docInfo;
	}

	public List<Map<String, Object>> getTrapList(String pestTrapClass) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("pestTrapClass", pestTrapClass);
		
		String sql = """
                select t.id as "Trap_id"
                , t."Type" as "TrapClass"
                , tc."Value" as "TrapClassName"
                , t."Code" as "TrapCode"
                , t."Name" as "TrapName"
                , t._order as "TrapOrder"
                , t."Type2" as "AreaClass"
                , lc."Value" as "AreaClassName"
                , t."Char1" as "FloorName"
	            from master_t t
				inner join user_code tc on t."Type" = tc."Code" and tc."Parent_id" = (select id from user_code where "Code" = 'pest_trap_class' and "Parent_id" is null)
				inner join sys_code lc on t."Type2" = lc."Code" and lc."CodeType" = 'haccp_area_class'
	            where t."MasterClass" = 'pest_trap'
                and t."Type" = :pestTrapClass
                order by t._order
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
		return items;
	}

	public List<Map<String, Object>> getPestList(String pestTrapClass) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("pestTrapClass", pestTrapClass);
		
		String sql = """
		        select c.id as TrapClass_id, c."Code" as "TrapClass", c."Value" as "TrapClassName", c._order as "TrapClassOrder"
				, m."Type" as "PestClass", sc."Value" as "PestClassName"
	            , m.id as "Pest_id", m."Code" as "PestCode", m."Name" as "PestName", rd._order as PestOrder
	            from user_code c
	            inner join rela_data rd on rd."TableName1" = 'user_code' and rd."RelationName" = 'trap_class-pest' and c.id = rd."DataPk1"
	            inner join master_t m on rd."TableName2" = 'master_t' and rd."RelationName" = 'trap_class-pest' and m.id = rd."DataPk2"
				inner join sys_code sc on m."Type" = sc."Code" and sc."CodeType" = 'pest_class'
	            where m."MasterClass" = 'pest'
	            and c."Code" = :pestTrapClass
                order by rd._order
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
		return items;
	}

	public Map<String, Object> getPestControlResult(Integer bhId, String dataDate, String pestTrapClass,
			String pestIdList, Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("bhId", bhId);
		paramMap.addValue("dataDate", dataDate);
		paramMap.addValue("pestTrapClass", pestTrapClass);
		paramMap.addValue("pestIdList", pestIdList);
		
		Map<String, Object> item = new HashMap<String, Object>();
		
		String sql = "";
		
		Map<String, Object> headInfo = null;
		
		if (bhId > 0) {
			sql = """
                    select b.id, b."Char1" as "Title", to_char(b."Date1", 'yyyy-MM-dd') as "DataDate", b."Char2" as "PestTrapClass"
                    , coalesce(uu."Name", cu."Name") as "FirstName", coalesce(r."State", 'write') as "State", coalesce(r."StateName", '상신대기') as "StateName"
                    from bundle_head b
					inner join user_profile cu on b._creater_id = cu."User_id"
					left join user_profile uu on b._modifier_id = uu."User_id"
					left join v_appr_result r on b.id = r."SourceDataPk" and r."SourceTableName" = 'bundle_head'
					where b."TableName" = 'check_result_pest_control_result'
                    and b.id = :bhId
				    """;
			
			headInfo = this.sqlRunner.getRow(sql, paramMap);
			
		} else {
			
			
			Map<String, Object> head = new HashMap<String, Object>();
			head.put("id", 0);
			head.put("Title", "방충방서점검일지("+ dataDate + ")");
			head.put("DataDate", dataDate);
			head.put("PestTrapClass", pestTrapClass);
			head.put("FirstName", user.getUserProfile().getName());
			head.put("State", "write");
			head.put("StateName", "상신대기");
			
			headInfo = head;
		}
		
		sql = """
               select pcs.id, pcs."HaccpAreaClassCode" as haccp_area_class_code
                , pcs."PestClassCode", pcs."SeasonCode"
                , fn_code_name('haccp_area_class', pcs."HaccpAreaClassCode") haccp_area_class
                , fn_code_name('pest_class', pcs."PestClassCode") as pest_class
                , fn_code_name('pest_season', pcs."SeasonCode") as season
                , pcs."FromCount" as from_count, pcs."ToCount" as to_count, pcs."ActionContent" as action_content
	            from pest_control_standard pcs
				""";
		
		List<Map<String, Object>> standardInfo = this.sqlRunner.getRows(sql, paramMap);
		
		sql = """
              with trap as (
		            select id as "Trap_id", "Type" as "TrapClass", "Code" as "TrapCode", "Name" as "TrapName", _order as "TrapOrder"
		            , "Char1" as "FloorName", "Type2" as "AreaClass"
		            from master_t
		            where "MasterClass" = 'pest_trap'
	                and "Type" = :pestTrapClass
                )
                , pest as (
		            select c.id as "TrapClass_id", c."Code" as "TrapClass", c."Value" as "TrapClassName", c._order as "TrapClassOrder"
		             , m.id as "Pest_id", m."Code" as "PestCode", m."Name" as "PestName", rd._order as "PestOrder"
	                from user_code c
	                inner join rela_data rd on rd."TableName1" = 'user_code' and rd."RelationName" = 'trap_class-pest' and c.id = rd."DataPk1"
	                inner join master_t m on rd."TableName2" = 'master_t' and rd."RelationName" = 'trap_class-pest' and m.id = rd."DataPk2"
	                where m."MasterClass" = 'pest'
	                and c."Code" = :pestTrapClass
                """;
				
		if (bhId == 0) {
			sql += " and m.\"StartDate\" <= cast(:dataDate as date) and m.\"EndDate\" >= cast(:dataDate as date)";
		}
		sql +=  """
				)
                , rslt as (	                
                    select r.id as "Result_id", r."Char1" as "StdTxt", r."Char2" as "DeviTxt", r."Number1" as "Trap_id", r."Number2" as "TotalResult", rr."DataPk2" as "Pest_id", rr."Char1" as "ResultValue"
	                from check_mast m
	                inner join check_result r on m.id = r."CheckMaster_id"
	                inner join rela_data rr on rr."TableName1" = 'check_result' and rr."TableName2" = 'master_t' and rr."RelationName" = 'check_result-pest' and r.id = rr."DataPk1"
	                where m."Code" = '방충방서점검'
	                and exists (
	                	select 1
	                	from rela_data
	                	where "TableName1" = 'bundle_head'
	                	and "TableName2" = 'check_result'
	                	and "DataPk1" = :bhId
	                	and "DataPk2" = r.id
	                )
                )
                select t."Trap_id", t."TrapClass", t."TrapCode", t."TrapName", t."TrapOrder", t."FloorName", t."AreaClass"
			  """;
		
		if (StringUtils.hasText(pestIdList)) {
			String pestIdArr[] = pestIdList.split(",");
			
			for (int i = 0; i < pestIdArr.length; i++) {
				sql += " , max(case when p.\"Pest_id\" = " + pestIdArr[i] + " then r.\"ResultValue\" else null end) as pest_" + pestIdArr[i] ;
			}
		}
		
		sql += """
                , max(r."TotalResult") as "PestTotal"
                , max(r."StdTxt") as "StdTxt"
                , max(r."DeviTxt") as "DeviTxt"
                from trap t
		        inner join pest p on t."TrapClass" = p."TrapClass"
		        left join rslt r on t."Trap_id" = r."Trap_id" and p."Pest_id" = r."Pest_id"
		        group by t."Trap_id", t."TrapClass", t."TrapCode", t."TrapName", t."TrapOrder", t."FloorName", t."AreaClass"
                order by t."TrapOrder"
				
			   """;
		
		List<Map<String, Object>> pestInfo = this.sqlRunner.getRows(sql, paramMap);
		
		item.put("head_info", headInfo);
		item.put("standard_info", standardInfo);
		item.put("pest_info", pestInfo);
		
		return item;
	}

	public List<Map<String, Object>> getFloorList(String pestTrapClass) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("pestTrapClass", pestTrapClass);
		
		String sql = """
					with t as (
		                select 0 as rn, '전체' as "Char1"
		                union all
		                select rank() over(order by case when "Char1"='지하' then '0층' else "Char1" end) 
			                , t."Char1"
		                from master_t t
		                where t."MasterClass" = 'pest_trap'     
		                and t."Type" = :pestTrapClass
		                group by t."Char1"
		            )
		            select *
		            from t
		            order by rn
		            """;
		
		List<Map<String, Object>> item = this.sqlRunner.getRows(sql, paramMap);
		
		return item;
	}

}
