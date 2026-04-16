package mes.app.definition.service.material;

import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class RoutingByMatService {
	
	@Autowired
	SqlRunner sqlRunner;

	public List<Map<String, Object>> getRoutingProcessList(String routingPk){
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("routing_pk", CommonUtil.tryInt(routingPk));
        
        String sql = """
			select rp."ProcessOrder" as proc_order, p."Code" as proc_code, p."Name" as proc_name, p."ProcessType" as proc_type
	        from routing_proc rp 
	        inner join process p on p.id = rp."Process_id"
	        where rp."Routing_id" = :routing_pk
	        order by rp."ProcessOrder" 
        """;
        	
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        return items;
    };
}
