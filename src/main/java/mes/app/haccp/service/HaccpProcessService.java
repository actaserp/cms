package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class HaccpProcessService {

	@Autowired
	SqlRunner sqlRunner;

	// HACCP 공정 목록 조회
	public List<Map<String, Object>> getHaccpProcessList(String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("keyword", keyword);
        
        String sql = """
		with aa as(
		select 
		rd."DataPk1" as hp_id
		,string_agg(p."Name", ',') as process_name
		from rela_data rd 
		inner join process p on p.id = rd."DataPk2" 
		 where rd."TableName1" ='haccp_proc'
		group by rd."DataPk1" 
		)
		select hp.id as hp_id
		, hp."Code" as haccp_process_code
		, hp."Name" as haccp_process_name
		, aa.process_name
		, hp."Description"
		--, hp."MonitoringMethod"
		--, hp."ActionMethod"
		--, hp."TestCycle"
		--, hp."Standard"
		, hp."ProcessKind"
		, fn_code_name('happc_process_kind', hp."ProcessKind") as "ProcessKindName"
		, to_char(hp."_created",'YYYY-MM-DD HH24:MI:SS') as "_created" 
		FROM haccp_proc hp
		left join aa on aa.hp_id= hp.id
		where 1=1
        """;
        
        if (StringUtils.isEmpty(keyword) == false) {
        	sql += " and upper(hp.\"Name\") like concat('%%',upper(:keyword),'%%') ";
        }
                
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// 기본정보 조회
	public Map<String, Object> getHaccpProcessDetail(Integer hp_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("hp_id", hp_id);
        
        String sql = """
        		select hp.id as hp_id
	            , hp."Code" as haccp_process_code
	            , hp."Name" as haccp_process_name
	            , hp."Description"
	            , hp."MonitoringMethod"
	            , hp."ActionMethod"
	            , hp."TestCycle"
	            , hp."ProcessKind"
	            , hp."Standard"
	            , to_char(hp."_created",'YYYY-MM-DD HH24:MI:SS') as "_created" 
	            FROM haccp_proc hp 
	            where hp.id = :hp_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        
        return item;
	}
	
	/**
	 * haccp_proc 별 실제 공정 맵핑
	 * @param hp_id
	 * @return
	 */
	public List<Map<String, Object>> getHaccpProcessAndProcessList(Integer hp_id){		
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("hp_id", hp_id);
		
		String sql = """				
		select 
		p.id as value
		, p."Name" as text 
		, hp.id as hp_id
		, hp."Name" as hp_name
		, hp."Code" as hp_code
		from haccp_proc hp 
		inner join rela_data rd on hp.id=rd."DataPk1" and rd."TableName1" ='haccp_proc' and rd."TableName2" ='process'
		inner join process p on p.id = rd."DataPk2"		
		where hp.id=:hp_id
		""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
		

		return items;		
	}
	

	// HACCP 공정별항목 조회
	public List<Map<String, Object>> getHaccpProcessItemList(Integer hp_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hp_id", hp_id);
        
        String sql = """
        		select hpi.id as hpi_id
	            , hpi."HaccpProcess_id" as hp_id
	            , hpi."HaccpItem_id" as item_id
	            , hp."Name" as haccp_process_name
	            , hi."Name" as item_name
	            , hi."ResultType"
	            , u."Name" as unit_name
	            , hpi."_order"
	            , to_char(hpi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
	            FROM haccp_proc_item hpi 
	            left join haccp_proc hp on hp.id = hpi."HaccpProcess_id" 
	            left join haccp_item hi on hi.id = hpi."HaccpItem_id" 
	            left join unit u on u.id = hi."Unit_id"
	            where hpi."HaccpProcess_id" = :hp_id
	            order by hpi."_order" 
	            """;

        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}	
}
