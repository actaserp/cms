package mes.app.definition.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class AreaService {
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getAreaList(String txtName, String spjangcd) {
		
		String sql = """
				select A.id
                , F."Name" as factory_name
                , A."Parent_id" as parent_id
                , A."Name" as name
                , A."Description" as description
                , A."Factory_id" as factory_id
                from area A
                left join factory F on F.id = A."Factory_id"
                where A."Name" like concat('%',:txtName,'%')
                AND A.spjangcd = :spjangcd
                order by  A."Name"
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("txtName", txtName);
		dicParam.addValue("spjangcd", spjangcd);
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
	}
	
	
	public Map<String, Object> getArea(int id){
		
		String sql = """
				select A.id
                , A."Factory_id" as factory_id
                , F."Name" as factory_name
                , A."Parent_id" as parent_id
                , P."Name" as parent_name
                , A."Name" as name
                , A."Description" as description
                from area A
                left join factory F on F.id = A."Factory_id"
                left join area P on P.id = A."Parent_id"
                where A.id = :pk
				""";
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("pk", id);
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
}