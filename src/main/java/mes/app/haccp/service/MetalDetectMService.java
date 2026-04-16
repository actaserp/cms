package mes.app.haccp.service;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class MetalDetectMService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	public List<Map<String, Object>> getMetalDetectM() {

		String sql = """
				    select m.id, m."Type" as type, m."Code" as code, m."Name" as name
			        , m."TestCount" as test_count, m."ProductionTestCycle" as production_test_cycle
			        , m."TestPiece" as test_piece, m."Description" as description
			        from metal_detect_m m
			        order by m."Type", m."Name"
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, null);
        
        return items;
	}

	public Map<String, Object> getMetalDetectMInfo(int id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id); 
		
		String sql = """
				select m.id, m."Type", m."Code", m."Name"
		        , m."TestCount", m."ProductionTestCycle"
		        , m."TestPiece", m."Description"
		        from metal_detect_m m
		        where m.id = :id
 				""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);
        
        return items;
	}

	public List<Map<String, Object>> detailList(Integer masterId) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("masterId", masterId); 
		
		String sql = """
				select d.id, d._order, d."TestTarget" as test_target
            , d."PiecePosition1" as piece_position1, d."PiecePosition2" as piece_position2
	        , (select id
		        from attach_file f
		        where f."TableName" = 'metal_detect_m_detail'
		        and f."DataPk" = d.id limit 1
		        ) as file_id
		        from metal_detect_m_detail d
		        where d."MetalDetectMaster_id" = :masterId
		        order by d._order
				""";
		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);
        
        return items;
	}

}
