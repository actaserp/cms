package mes.app.test.service;

import java.util.List;
import java.util.Map;

import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class TestMasterService {

	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getTestMasterList(String name, String testClass) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("name", name);
		dicParam.addValue("test_class", testClass);
		
		
		String sql = """
			select tm.id
            , tmg."Name" as test_master_grp
            , sc."Value" as test_master_class
            , tm."Name" as test_master_name
            , case when tm."TestType" = 'no_use_item_master' then 'N'
	            when tm."TestType" = 'use_item_master' then 'Y' end as use_test_item
            from test_mast tm
            inner join test_mast_grp tmg on tmg.id = tm."TestMasterGroup_id"
            inner join sys_code sc on sc."Code" = tmg."TestClass"
            where sc."CodeType" = 'test_class'
				""";
		
		if (StringUtils.isEmpty(name) == false) sql += " and tm.\"Name\"  like concat('%%',:name,'%%') ";
				
		if(StringUtils.isEmpty(testClass) == false) sql += " and tmg.\"TestClass\" = :test_class ";
		
		sql += " order by tm.\"TestType\", tm.id ";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
		
	}
	
	public Map<String, Object> getTestMasterDetail(int id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("id", id);
		
		String sql = """
			select tm.id
            , tm."TestMasterGroup_id" as master_grp
            , tm."Name" as master_name
            , tm."TestType" as test_type
            from test_mast tm
            where tm.id = :id
				""";
		
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
	
	
	public List<Map<String, Object>> getAppliedMat(int id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("master_id", id);
		
		String sql = """
		 with A as (
	                select "Material_id" as mat_id
	                from test_mast_mat tmm
	                where "TestMaster_id" = :master_id
                ), B as (
	                select A.mat_id as id
	                , fn_code_name('mat_type', min(mg."MaterialType")) as mat_type
	                , min(mg."Name") as mat_grp
	                , min(m."Code") as mat_code
	                , min(m."Name") as mat_name  
	                from A
	                inner join material m on m.id = A.mat_id
	                left join mat_grp mg on mg.id = m."MaterialGroup_id"
	                group by A.mat_id
                )
                select * from B		
				""";
		
		List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
		
		
	}
	
	
	public Map<String, Object> getItemInfo(int item_id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("item_id", item_id);
		
		String sql = """
		select ti.id as test_item_id
            , ti."Name" as test_item_name
            , sc."Value" as result_type
            , ti."RoundDigit" as round_digit
            from test_item ti
            inner join sys_code sc on sc."Code" = ti."ResultType"
            where sc."CodeType" = 'result_type'
            and ti.id = :item_id		
			""";
		Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
		return item;
	}
	
	
	public List<Map<String,Object>> getItemList(int master_id) {
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("master_id", master_id);
		String sql = """
			select tim.id as id
            , ti.id as test_item_id
            , ti."Name" as test_name
            , sc."Value" as test_res_type
            , tim."RoundDigit" as round_digit
            , tim."SpecType" as spec_type
            , tim."LowSpec" as low_spec
            , tim."UpperSpec" as upper_spec 
            , tim."SpecText" as spec_text
            , tim."EngSpecText" as eng_spec_text
            from test_item_mast tim 
            inner join test_item ti on ti.id = tim."TestItem_id"
            inner join sys_code sc on sc."Code" = ti."ResultType"
            where sc."CodeType" = 'result_type'
            and tim."TestMaster_id" = :master_id
            order by "_order"
				""";
		
		List<Map<String,Object>> items = this.sqlRunner.getRows(sql, dicParam);
		return items;
		
	}

	public void deleteByMasterId(Integer master_id) {
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("master_id", master_id);
		String sql = """
				delete from test_item_mast
				where "TestMaster_id" = :master_id
				""";
		
		this.sqlRunner.execute(sql, dicParam);
	}
	
}