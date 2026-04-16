package mes.app.precedence.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import mes.domain.services.SqlRunner;

@Service
public class AirFilterReplaceService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> yearMonthPlanResultSheet(String dataYear, String period) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("dataYear", dataYear);  
		paramMap.addValue("period", period);  
		
		String sql = """
				with M as (
	            select m.id as master_id, m."Code" as master_code, m."Name" as master_name, m."Description" as master_descr
	            , m."Type" as master_type, m."Type2" as master_type2, m."Char2" as master_period
	            from master_t m
	            where m."MasterClass" = 'air_filter'
	            """;
		
		if (StringUtils.hasText(period)) {
			sql += " and m.\"Char2\" like concat('%%',:period,'%%') ";
		}
		sql += """
            ), MP as (
	            select M.master_id
	            , mp."DataMonth", mp."PlanYN",mp."ResultYN", mp.id
	            from M
	            inner join master_year_month_plan mp on mp."MasterTable_id" = m.master_id
	            and mp."DataYear" = cast(:dataYear as Integer)
	            where 1 = 1
            ), F as (
	            select MP.master_id, MP.id, F.id as file_id1, F."FileIndex" as file_index1, F2.id as file_id2, F2."FileIndex" as file_index2
	            from MP
	            left join attach_file F on F."DataPk" = MP.id and F."TableName" = 'master_year_month_plan' 
	            and F."AttachName" = 'replace_filter1'
	            left join attach_file F2 on F2."DataPk" = MP.id and F2."TableName" = 'master_year_month_plan'
	            and F2."AttachName" = 'replace_filter2'
            )
            select M.master_id, M.master_code, M.master_name, M.master_descr, M.master_type, M.master_type2,  M.master_period
				""";
		
		for (int i = 1; i < 13; i++) {
            sql += ", min(case when mp.\"DataMonth\" = " + i + " then mp.\"PlanYN\" end) as p" + i 
            	 + ", min(case when mp.\"DataMonth\" = " + i + "  then mp.\"ResultYN\" end) as r" + i
            	 + ", min(case when mp.\"DataMonth\" = " + i + "  then mp.id end) as d" + i
                 + ", min(case when mp.\"DataMonth\" = " + i + "  then case when mp.\"ResultYN\" = 'Y' then '●' when mp.\"PlanYN\" = 'Y' then '○' end end) as pr" + i + " "
                 + ", min(case when mp.\"DataMonth\" = " + i + "  and F.file_index1 = " + i + " then F.file_id1 end) as file1_" + i + " "
                 + ", min(case when mp.\"DataMonth\" = " + i + "  and F.file_index2 = " + i + " then F.file_id2 end) as file2_" + i + " "
                 ;
		}
		
		sql += """
				from M
	            left join MP on MP.master_id = M.master_id
	            left join F on F.master_id = MP.master_id
	            where 1 = 1
	            group by M.master_id, M.master_code, M.master_name, M.master_descr, M.master_type, M.master_type2, M.master_period
	           """;
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public List<Map<String, Object>> yearMonthPlanResultList(String dataYear) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("dataYear", dataYear); 
		
		String sql = """
				with M as (
	            select m.id as master_id, m."Code" as master_code, m."Name" as master_name, m."Description" as master_descr
	            , m."Type" as master_type, m."Type2" as master_type2
	            from master_t m
	            where m."MasterClass" = 'air_filter'
            ), MP as (
	            select M.master_id, mp."DataYear"
	            , mp."DataMonth", mp."PlanYN",mp."ResultYN", mp.id
	            from M
	            inner join master_year_month_plan mp on mp."MasterTable_id" = m.master_id
	            and mp."DataYear" = cast(:dataYear as Integer)
	            where 1 = 1
            )
            select mp.id , M.master_id, M.master_code, M.master_name, M.master_descr, M.master_type, M.master_type2
            ,mp."DataYear" as data_year, mp."DataMonth" as data_month, mp."PlanYN" as plan_yn, mp."ResultYN" as result_yn
            , case when mp."PlanYN" ='Y' then '○' end as plan_signal
			, case when mp."ResultYN" = 'Y' then '●' end as plan_result
	           from M
	            inner join MP on MP.master_id = M.master_id
	            where 1 = 1
                and (mp."PlanYN" = 'Y' or mp."ResultYN" = 'Y')
				""";
		
		// 파일관련 내용 추가해야함

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> getAirFilterDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);  
		
		String sql = """
				select mp.id, M.id as master_id, m."Code", m."Name", m."Description"
	            , m."Type", m."Type2"
	            , mp."DataYear"
	            , mp."DataMonth", mp."PlanYN",mp."ResultYN"
		        , (select id
			        from attach_file f
			        where f."TableName" = 'master_year_month_plan'
			        and f."DataPk" = mp.id order by f.id desc limit 1
			        ) as file_id
	            from master_t m
	            inner join master_year_month_plan mp on mp."MasterTable_id" = m.id
	            where mp.id = :id
	            and m."MasterClass" = 'air_filter'
				""";
		
		// 파일관련 내용 추가해야함
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}


}
