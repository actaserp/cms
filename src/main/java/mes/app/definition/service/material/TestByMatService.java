package mes.app.definition.service.material;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TestByMatService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTestMasterList(int matPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPk);
        
        String sql = """
			select tm.id, fn_code_name('test_class', tg."TestClass") as test_class_name
	            , tg."Name" as test_master_group_name, t."Name" as test_master_name 
	            , t.id as test_master_id
	            from test_mast_mat tm 
	            inner join test_mast t on t.id = tm."TestMaster_id" 
	            left join test_mast_grp tg on tg.id = t."TestMasterGroup_id" 
	            where tm."Material_id" = :mat_pk
                order by tg."TestClass", tg."Name", t."Name" 
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    };
}
