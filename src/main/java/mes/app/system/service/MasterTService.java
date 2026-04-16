package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MasterTService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMasterT(String masterClass, String typeClassCode, String typeClassTable,
			String baseDate, String type2ClassCode, String type2ClassTable, String type, String type2) {

		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("masterClass", masterClass);  
		paramMap.addValue("typeClassCode", typeClassCode);  
		paramMap.addValue("typeClassTable", typeClassTable);  
		paramMap.addValue("baseDate", baseDate);
		paramMap.addValue("type2ClassCode", type2ClassCode);
		paramMap.addValue("type2ClassTable", type2ClassTable);
		paramMap.addValue("type", type);
		paramMap.addValue("type2", type2);
		
		String funcName = "fn_code_name";
		String funcName2 = "fn_code_name";
		
        if (typeClassTable.equals("user_code")) {
        	funcName = "fn_user_code_name";
        } 
        
        if (type2ClassTable != null) {
        	if(type2ClassTable.equals("user_code")) {
        		funcName2 = "fn_user_code_name";
        	}
        }
        String sql = """
        		select c.id
	            , c."MasterClass" as master_class, c."Code" as code
	            , c."Name" as name
	            , c."Type" as code_type
	            , c."Type2" as code_type2
	            , c."Description" as description
	            , c."Char1" as char1
	            , c."Char2" as char2
	            , c."Char1" as area_num
	            , c."Number1" as number1
	            , c."Date1" as date1
	            , c."Text1" as text1
	            , c."StartDate" as start_date
	            , c."EndDate" as end_date
	            """;
        
		    sql += "," + funcName + "(:typeClassCode, c.\"Type\") as code_type_name ";
		    sql += "," + funcName2 + "(:type2ClassCode, c.\"Type2\") as code_type_name2 ";
 		    sql += " from master_t c where c.\"MasterClass\" = :masterClass and cast(:baseDate as date) between coalesce(c.\"StartDate\",'2000-01-01') and coalesce(c.\"EndDate\",'2100-12-31') ";
		    
 		    if(type != null && !type.equals("")) sql += " and c.\"Type\" = :type ";
 		    if(type2 != null && !type2.equals("")) sql += " and c.\"Type2\" = :type2 ";
 		    
 		    sql += " order by c._order, c.\"Type\", c.\"Type2\" ";
 		    
	        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
	        
	        return items;
	}

	public Map<String, Object> getMasterTDetail(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);  
		
		String sql = """
				select c.id
                , c."MasterClass"
                , c."Code"
                , c."Name"
                , c."Type"
                , c."Type2"
                , c."Char1"
                , c."Char2"
                , c."Char1" as area_num
                , c."Number1"
                , c."Date1"
                , c."Text1"
                , c."Description"
                , c."StartDate"
                , c."EndDate"
	            from master_t C
	            where C.id = cast(:id as Integer)
 				""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}
	


}
