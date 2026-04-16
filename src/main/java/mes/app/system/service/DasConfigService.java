package mes.app.system.service;

import java.util.List;
import java.util.Map;

import mes.app.common.TenantContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.domain.services.SqlRunner;

@Service
public class DasConfigService {
	
	@Autowired
	SqlRunner sqlRunner;
	
	// searchMainData
	public List<Map<String, Object>> getDasConfigList(Integer server_id, Integer equipment_id){
        String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("server_id", server_id);
        dicParam.addValue("equipment_id", equipment_id);
        dicParam.addValue("spjangcd", tenantId);
        String sql = """
			select 
                  dc.id,
                  dc."Name",
                  dc."Description",
                  dc."Handler",
                  dc."DeviceType",
                  dc."Configuration",
                  dc."ConfigFileName",
                  dc."Topic",
                  dc."Equipment_id",
                  e."Name" as equipment_name,
                  dc."Server_id" ,
                  --case when dc.is_active is true then 'Y' else 'N' end as is_active,
                  dc.is_active,
                  to_char(dc._created ,'yyyy-mm-dd hh24:mi:ss') as created,
                  ds."Name" as server_name
                from das_config dc
                    left outer join das_server ds on dc."Server_id" = ds.id 
                    left outer join equ e on e.id = dc."Equipment_id"
                where 1=1
                and dc.spjangcd = :spjangcd
		    """;
        
        if (server_id != null) {
        	sql += "  and dc.\"Server_id\"= :server_id ";
        }
        
        if (equipment_id!=null){            	
            sql+= " and dc.\"Equipment_id\"= :equipment_id ";
        }
        
        sql += "order by dc.\"Name\" ";
        
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);
        
        return items;
	}
	
	// // showDetail
	public Map<String, Object> getDasConfigDetail(Integer id){
        String tenantId = TenantContext.get();
		MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("id", id);
        dicParam.addValue("spjangcd", tenantId);
        String sql = """
			select 
                  dc.id,
                  dc."Name",
                  dc."Description",
                  dc."Handler",
                  dc."DeviceType",
                  dc."Configuration",
                  dc."ConfigFileName",
                  dc."Topic",
                  dc."Equipment_id",
                  e."Name" as equipment_name,
                  dc."Server_id" ,
                  --case when dc.is_active is true then 'Y' else 'N' end as is_active,
                  dc.is_active,
                  to_char(dc._created ,'yyyy-mm-dd hh24:mi:ss') as created,
                  ds."Name" as server_name
                from das_config dc
                    left outer join das_server ds on dc."Server_id" = ds.id 
                    left outer join equ e on e.id = dc."Equipment_id"
                where dc.id = :id
                and dc.spjangcd = :spjangcd
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}
}
