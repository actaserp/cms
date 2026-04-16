package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class UserCodeService {
	
	@Autowired 
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getCodeList(String txtCode){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("txtCode", txtCode);
		
		String sql = """
				select id
                , "Parent_id" as parent_id
                , "Code" as code
                , "Value" as name
                ,"Description" as description
                from user_code
                where "Value" ilike concat('%%',:txtCode,'%%')
                order by "Value"
				""";
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	
	}


	public List<Map<String, Object>> getSystemCodeList(String txtCode,String txtCodeType, String spjangcd){

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("txtCode", txtCode);
		dicParam.addValue("txtCodeType", txtCodeType);
		dicParam.addValue("spjangcd", spjangcd);

		String sql = """
				select id
				, "CodeType" as code_type
                , "Code" as code
                , "Value" as name
                ,"Description" as description
                from sys_code
                where "Value" ilike concat('%%',:txtCode,'%%')
                AND "Description" ilike concat('%%',:txtCodeType,'%%')
                AND spjangcd = :spjangcd
				""";
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;

	}

	public Map<String, Object> getCode(int id) {
		String sql = """
				select C.id
            , C."Parent_id" as parent_id
            , P."Value" as parent_name
            , C."Code" as code
            , C."Value" as name
            , C."Description" as description
            from user_code C
            left join user_code P on P.id = C."Parent_id"
            where C.id = :id
			""";
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}


	public Map<String, Object> getSystemcCode(int id, String spjangcd) {
		String sql = """
				select S.id
			, S."CodeType" as code_type
            , S."Code" as code
            , S."Value" as name
            , S."Description" as description
            from sys_code S
            where S.id = :id
            AND S.spjangcd = :spjangcd
			""";

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		dicParam.addValue("spjangcd", spjangcd);

		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	

	public List<Map<String, Object>> getUserCode(String parentCode, String baseDate, String type, String typeClassCode, String type2ClassCode, String typeClassTable, String type2ClassTable) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("parentCode", parentCode);  
		paramMap.addValue("baseDate", baseDate);
		paramMap.addValue("type", type);  
		paramMap.addValue("typeClassCode", typeClassCode);
		paramMap.addValue("type2ClassCode", type2ClassCode);  
		paramMap.addValue("typeClassTable", typeClassTable);
		paramMap.addValue("type2ClassTable", type2ClassTable);  
		
		String funcName = "fn_code_name";
		String funcName2 = "fn_code_name";
		
		if (typeClassTable != null) {
			if(typeClassTable.equals("user_code")) {
				funcName = "fn_user_code_name";
			}
		}
		
		if (type2ClassTable != null) {
			if(type2ClassTable.equals("user_code")) {
				funcName2 = "fn_user_code_name";
			}
		}
		
		
		String sql = """
				select c.id
	            , c."Parent_id" as parent_id, c."Code" as code
	            , c."Value" as name
	            , c."Type" as code_type
	            , c."Type2" as code_type2
	            , c."Description" as description
	            , c."StartDate" as start_date
	            , c."EndDate" as end_date
				""";
		if (typeClassCode != null) {
			sql += "," + funcName + "(:typeClassCode, c.\"Type\") as code_type_name ";			
		}
		
		if (type2ClassCode != null) {
			sql += "," + funcName2 + "(:type2ClassCode, c.\"Type2\") as code_type_name2 ";	
		}
		sql += """
			from user_code c
            inner join user_code pc on pc.id = c."Parent_id"
            where pc."Code" = :parentCode
            and cast(:baseDate as date) between coalesce(c."StartDate",'2000-01-01') and coalesce(c."EndDate",'2100-12-31')
			""";
		
		if (type != null) {
			sql += " and c.\"Type\" = :type "; 
		}
		
		sql += " order by c.\"Type\", c._order ";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> userCodeDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);  
		
		String sql = """
				select c.id
                , c."Parent_id"
                , c."Code"
                , c."Value"
                , c."Type"
                , c."Type2"
                , c."Description"
                , c."StartDate"
                , c."EndDate"
		            from user_code C
		            where C.id = :id
				""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}

	public List<Map<String, Object>> relationDataList(String tableName2, String relationName, String baseId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("tableName2", tableName2);  
		paramMap.addValue("relationName", relationName);  
		paramMap.addValue("baseId", baseId);
		
		String sql = "";
		if(tableName2.equals("master_t")) {
			sql = """
				select rd.id, c.id as data_pk2
                , c."Code" as code, c."Name" as name
                , rd._order
                , rd."StartDate" as start_date, rd."EndDate" as end_date 
	            from rela_data rd 
	            inner join master_t c on c.id = rd."DataPk2"
	            and rd."TableName2" = 'master_t'
	            where "DataPk1" = cast(:baseId as Integer)
	            and rd."TableName1" = 'user_code'
	            and rd."RelationName" = :relationName
	            order by rd._order, rd.id 
				""";
		} else if (tableName2.equals("user_code"))
			sql = """
				select rd.id, c.id as data_pk2
                , c."Code" as code, c."Value" as name
                , rd._order
                , rd."StartDate" as start_date, rd."EndDate" as end_date 
	            from rela_data rd 
	            inner join user_code c on c.id = rd."DataPk2"
	            and rd."TableName2" = 'user_code'
	            where "DataPk1" = cast(:baseId as Integer)
	            and rd."TableName1" = 'user_code'
	            and rd."RelationName" = :relationName
	            order by rd._order, rd.id 
			     """;
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

	public Map<String, Object> getValue(String code) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("code", code);
		String sql = """
				select "Value"
				from sys_code 
				where "Code" = :code;
			""";
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
}