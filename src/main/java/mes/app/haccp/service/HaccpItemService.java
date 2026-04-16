package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class HaccpItemService {

	@Autowired
	SqlRunner sqlRunner;
	
	// 조회
	public List<Map<String, Object>> getHaccpItemList(String keyword) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();
		paramMap.addValue("keyword", keyword);
        
        String sql = """
        		SELECT  hi.id
	            , hi."Code" as item_code
	            , hi."Name" as item_name
	            , hi."ResultType"
	            , fn_code_name('haccp_result_type', hi."ResultType") as "ResultTypeName"
	            , hi."Unit_id"
	            , to_char(hi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created" 
	            , u."Name" as unit_name
	            FROM haccp_item hi
	            left join unit u on u.id = hi."Unit_id" 
	            where 1=1
	            """;
        
        if (StringUtils.isEmpty(keyword) == false) {
        	sql += " and upper(hi.\"Name\") like concat('%%',upper(:keyword),'%%') ";
        }
        
        sql += " order by hi.\"Name\" ";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}
	
	// 상세조회
	public Map<String, Object> getHaccpItemDetail(Integer item_id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("item_id", item_id);
        
        String sql = """
        		select hi.id
            , hi."Code" as item_code
            , hi."Name" as item_name
            , hi."ResultType"
            , fn_code_name('haccp_result_type', hi."ResultType") as "ResultTypeName"
            , hi."Unit_id"
            , to_char(hi."_created",'YYYY-MM-DD HH24:MI:SS') as "_created" 
            , u."Name" as unit_name
            FROM haccp_item hi
            left join unit u on u.id = hi."Unit_id" 
            where hi.id = :item_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);
        
        return item;
	}
}
