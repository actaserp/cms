package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class HaccpItemLimitService {

	@Autowired
	SqlRunner sqlRunner;

	// HACCP일지목록 조회
	public List<Map<String, Object>> getHaccpItemLimitList(Integer hp_id, Integer mat_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("hp_id", hp_id);
		paramMap.addValue("mat_id", mat_id);
        
        String sql = """
        		select hil.id as hil_id
	            , hil."HaccpProcess_id" as hp_id
	            , hil."HaccpItem_id" as item_id
	            , hil."Material_id"
	            , hpi.id as hpi_id
	            , m."Name" as product_name
	            , hp."Name" as haccp_process_name 
	            , hi."Name" as item_name
	            , fn_code_name('haccp_result_type', hi."ResultType") as "ResultTypeName"
	            , hil."LowSpec" 
	            , hil."UpperSpec" 
	            , hil."SpecText" 
	            , to_char(hil."_created",'YYYY-MM-DD HH24:MI:SS') as "_created"
	            , u."Name" as unit_name
	            , hpi."_order"
	            from haccp_item_limit hil
	            inner join haccp_proc hp on hp.id=hil."HaccpProcess_id"
	            inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hil."HaccpProcess_id" 
	            and hpi."HaccpItem_id" = hil."HaccpItem_id" 
	            inner join haccp_item hi on hi.id = hil."HaccpItem_id"
	            left join unit u on u.id = hi."Unit_id"
	            inner join material m on m.id = hil."Material_id"
	            where 1=1
	            """;
        
        if (hp_id != null) {
        	sql += " and hp.id = :hp_id ";
        }

        if (mat_id != null) {
        	sql += " and hil.\"Material_id\" = :mat_id ";
        }
        
        sql += " order by m.\"Name\", hpi.\"_order\" ";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	// HACCP 항목 기준 조회
	public List<Map<String, Object>> getHaccpItemLimitInputList(Integer hp_id, Integer mat_id) {
			
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("hp_id", hp_id);     
		paramMap.addValue("mat_id", mat_id);
        
        String sql = """
        		select hp.id as hp_id
		        , hi.id as item_id
		        , m.id as "Material_id"
		        , m."Name" as product_name
		        , hpi.id as hpi_id
		        , hp."Name" as haccp_process_name 
		        , hi."Name" as item_name
		        , hil.id as hil_id
		        , hil."LowSpec" 
		        , hil."UpperSpec" 
		        , hil."SpecText" 
		        , hpi."_order"
		        , u."Name" as unit_name
		        , u.id as unit_id
		        from haccp_proc hp
		        inner join haccp_proc_item hpi on hpi."HaccpProcess_id" = hp.id 
		        inner join haccp_item hi on hi.id=hpi."HaccpItem_id" 
		        left join unit u on u.id = hi."Unit_id"
		        inner join material m on m.id = :mat_id
		        left join haccp_item_limit hil on hil."HaccpProcess_id" = hp.id 
		        and hil."HaccpItem_id" = hi.id 
		        and m.id = hil."Material_id" 
		        where hp.id = :hp_id
		        order by hpi."_order"
		    """;
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
}
