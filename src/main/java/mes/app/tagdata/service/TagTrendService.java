package mes.app.tagdata.service;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;


import mes.domain.services.SqlRunner;

@Service
public class TagTrendService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTagTrendList(String data_from,String data_to,String tag_code){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("date_from", Timestamp.valueOf(data_from));
        dicParam.addValue("date_to", Timestamp.valueOf(data_to));
        dicParam.addValue("tag_code", tag_code);
        
        String sql = """
			with A as (
                select unnest(string_to_array(:tag_code, ';')) as tag_code
            )
			select T.tag_code, T.tag_name,  t."LSL" as lsl, t."USL" as usl
			from Tag T 
			inner join A on A.tag_code = T.tag_code 
        """;
        	
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        List<Map<String, Object>> items1 = null;
        List<Map<String,Object>> list = new ArrayList<Map<String,Object>>();
        Long lsl = null;
        Long usl = null;
        
        for(int i=0; i<items.size(); i++) {
        	String tagCode = items.get(i).get("tag_code").toString();
        	//String tagName = items.get(i).get("tag_name").toString();


			if (items.get(i).get("lsl") != null) {
				String lslString = items.get(i).get("lsl").toString();
				if (lslString.contains(".")) {
					// 소수점이 있는 경우 Double로 처리
					double lslDouble = Double.valueOf(lslString);
					lsl = (long) lslDouble; // 정수로 변환 (소수점 이하 버림)
				} else {
					// 정수인 경우 Long으로 변환
					lsl = Long.valueOf(lslString);
				}
			}

			if (items.get(i).get("usl") != null) {
				String uslString = items.get(i).get("usl").toString();
				if (uslString.contains(".")) {
					// 소수점이 있는 경우 Double로 처리
					double lslDouble = Double.valueOf(uslString);
					usl = (long) lslDouble; // 정수로 변환 (소수점 이하 버림)
				} else {
					// 정수인 경우 Long으로 변환
					usl = Long.valueOf(uslString);
				}
			}
        	
        	
        	dicParam.addValue("tagCode",tagCode);
        	dicParam.addValue("lsl",lsl);
        	dicParam.addValue("usl",usl);
        
        
        
	        String sql1 = """
	                with A as (
	                    select unnest(string_to_array(:tagCode, ';')) as tag_code
	                )
				    select td.tag_code, td.data_date, to_char(td.data_date, 'yyyy-mm-dd hh24:mi:ss') as data_time
	                , round(td.data_value::decimal, T."RoundDigit")::float as data_value
	                , t.tag_name
		            from tag_dat td 
	                inner join A on A.tag_code = td.tag_code
	                inner join tag T on T.tag_code = td.tag_code
	                where 1=1
	                and td.data_date between :date_from and :date_to
	                order by td.tag_code, td.data_date
	                """;
        
	        items1 = this.sqlRunner.getRows(sql1, dicParam);
	        
	        Map<String,Object> map = new HashMap<String,Object>();
        
	        if(items1.size() > 0) {
	        	map.put("tag_code", items1.get(0).get("tag_code").toString());
	            map.put("tag_name", items1.get(0).get("tag_name").toString());
	            map.put("data_value", items1.get(0).get("data_value").toString());
	            map.put("data_time", items1.get(0).get("data_time").toString());
	            map.put("data_date", items1.get(0).get("data_date").toString());
	            
	            list.add(map);
	            
	        }
        
        }
        
        
        
		return list;
    };
}
