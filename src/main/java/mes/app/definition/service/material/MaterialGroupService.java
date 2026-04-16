package mes.app.definition.service.material;


import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import io.micrometer.core.instrument.util.StringUtils;
import mes.domain.services.SqlRunner;

@Service
public class MaterialGroupService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	// 품목그룹 목록 조회
	public List<Map<String, Object>> getMatGrouptList(String matType, String matGrp,String spjangcd){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_type", matType);
        dicParam.addValue("mat_grp", matGrp);
		dicParam.addValue("spjangcd", spjangcd);

        String sql = """
			select mg.id
            , mg."Name" as material_group_name
            , mg."Code" as material_group_code
            , fn_code_name('mat_type', mg."MaterialType") as material_type
            from mat_grp mg 
            where 1=1
            AND spjangcd = :spjangcd
		    """;
        if (StringUtils.isEmpty(matType)==false) sql += " and mg.\"MaterialType\"= :mat_type ";
        if (StringUtils.isEmpty(matGrp)==false) sql += " and upper(mg.\"Name\") like concat('%%',upper( :mat_grp ),'%%') ";
        
        sql += "order by mg.id";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// 품목그룹 상세정보 조회
	public Map<String, Object> getMatGroupDetail(int matGrpId){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("group_id", matGrpId);
        
        String sql = """
			select mg.id
		    , mg."Name" as material_group_name
		    , mg."Code" as material_group_code
		    , mg."MaterialType" as material_type
		    from mat_grp mg 
		    where 1=1
		    and mg.id = :group_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
	
}
