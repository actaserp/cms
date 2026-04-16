package mes.app.tagdata.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TagCurrentService {

	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTagCurrentList(String action){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("action", action);
        
        String sql = """
			 with A as
	                (
	                select tag_code
	                --, max(to_char(data_date,'yyyy-mm-dd hh24:mi:ss')||'-'||data_value) as date_value
	                , max(data_date::text||'-'||data_value) as date_value
	                from tag_dat
	                where 1 = 1
	                group by tag_code
	                )
	                select A.tag_code as tag_code
                    , t.tag_name
                    , split_part(A.date_value, '-', 4) as data_value
                    ,substring(A.date_value, 1, 19) as data_date
                    ,concat(t."LSL", '~', t."USL") as spec
                    , e."Name" as equip_name
	                from A a
	                inner join tag t on a.tag_code = t.tag_code 
                    left join equ e on e.id = t."Equipment_id"
	                order by a.tag_code
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql,dicParam);
        return items;
    };
}
