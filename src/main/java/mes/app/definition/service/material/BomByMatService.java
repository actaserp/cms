package mes.app.definition.service.material;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class BomByMatService {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getBomListByMat(String matPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPk);
        
        String sql = """
			select bom.b_level as _level
                , m."Name" as mat_name
                , bom.bom_ratio
                , concat(bom.quantity,'/',bom.produced_qty) as bom_qty
                , fn_code_name('mat_type',mg."MaterialType") as mat_type
                , mat_pk, parent_mat_pk
                , u."Name" as unit
                , m."Code" as mat_code
                , bom.mat_pk as my_key
                , bom.parent_mat_pk as parent_key
	            from tbl_bom_detail(:mat_pk, to_char(now(),'yyyy-mm-dd')) as bom
                inner join material m on m.id = bom.mat_pk
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
	            order by tot_order
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
	
	public List<Map<String, Object>> getBomReverseListByMat(int matPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("mat_pk", matPk);
        
        String sql = """
			select bom.b_level as _level
                , m."Name" as mat_name
                , bom.bom_ratio
                , concat(bom.quantity,'->',bom.produced_qty) as bom_qty
                , fn_code_name('mat_type',mg."MaterialType") as mat_type
                , u."Name" as unit
                , m."Code" as mat_code
                , bom.prod_pk as my_key
                , bom.parent_prod_pk as parent_key
	            from tbl_bom_reverse(:mat_pk, to_char(now(),'yyyy-mm-dd')) as bom
                inner join material m on m.id = bom.prod_pk
                left join mat_grp mg on mg.id = m."MaterialGroup_id"
                left join unit u on u.id = m."Unit_id"
	            order by bom.tot_order
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
	}
}
