package mes.app.test.service;

import java.sql.Types;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class TestItemService {

	@Autowired
	SqlRunner sqlRunner;	

	// 목록 조회
	public List<Map<String, Object>> getTestItemList(String testMethodId, String testItemName, String testResType, Integer unit) {

		Integer testMethodIdInt = null;
		if (StringUtils.isNotEmpty(testMethodId)) {
			testMethodIdInt = Integer.valueOf(testMethodId);
		}

		MapSqlParameterSource dicParam = new MapSqlParameterSource();
		dicParam.addValue("test_method_id", testMethodIdInt, Types.INTEGER);
		dicParam.addValue("test_item_name", testItemName);
		dicParam.addValue("test_res_type", testResType);
		dicParam.addValue("unit", unit);

		StringBuilder sql = new StringBuilder("""
        select ti.id
             , ti."Code" as item_code
             , ti."Name" as item_name 
             , ti."EngName" as item_eng_name 
             , ti."ResultType" as res_type 
             , sc."Value" as item_res_type
             , ti."Unit_id" as item_unit 
             , ti."ItemType" as item_type
             , u."Name" as unit_name 
             , ti."RoundDigit" as item_round_digit 
             , tm."Name" as test_method_name
          from test_item ti 
          left join test_method tm on tm.id = ti."TestMethod_id"
          left join unit u on ti."Unit_id" = u.id 
         inner join sys_code sc on ti."ResultType"::text = sc."Code" 
         where sc."CodeType" = 'result_type'
    """);

		if (StringUtils.isNotEmpty(testMethodId))
			sql.append("and ti.\"TestMethod_id\" = :test_method_id ");

		if (StringUtils.isNotEmpty(testItemName))
			sql.append("and ti.\"Name\" like concat('%%', :test_item_name, '%%') ");

		if (StringUtils.isNotEmpty(testResType))
			sql.append("and ti.\"ResultType\" = :test_res_type ");

		if (unit != null)
			sql.append("and ti.\"Unit_id\" = :unit ");

		sql.append(" order by ti.id");

		List<Map<String, Object>> items = this.sqlRunner.getRows(sql.toString(), dicParam);

		return items;
	}


	// 상세정보 조회
	public Map<String, Object> getTestItemDetailItem(Integer id) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("id", id);
        
        String sql = """
            select ti.id, ti."Code" as item_code 
            , ti."Name" as item_name 
            , ti."EngName" as item_eng_name 
            , ti."ResultType" as res_type 
            , ti."Unit_id" as item_unit 
            , ti."ItemType" as item_type
            , ti."RoundDigit" as item_round_digit 
            , ti."TestMethod_id" as test_method_id
            , tm."Name" as test_method_name
            from test_item ti 
            left join test_method tm on tm.id = ti."TestMethod_id"
            where 1=1 and ti.id = :id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
	// 선택형결과 상세 정보 조회
	public List<Map<String, Object>> getTestItemDetailRes(Integer testItemId) {

		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("test_item_id", testItemId);
        
        String sql = """
            select trc.id 
            , trc."TestItem_id" as test_item_id 
            , trc."ResultCode" as result_code 
            , trc."ResultName" as result_name 
            , trc."PassYN" as pass_yn 
            from test_result_code trc 
            where 1=1 
            and trc."TestItem_id" = :test_item_id
            order by trc.id
		    """;
        
        List<Map<String, Object>> item = this.sqlRunner.getRows(sql, dicParam);
        
        return item;
	}
	
}
