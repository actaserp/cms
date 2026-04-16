package mes.app.check.service;

import java.io.IOException;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.Resource;
import org.springframework.core.io.UrlResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import mes.config.Settings;
import mes.domain.services.CommonUtil;
import mes.domain.services.SqlRunner;

@Service
public class CheckItemService {

	@Autowired
	SqlRunner sqlRunner;
	
	@Autowired
	Settings settings;
	
	public List<Map<String, Object>> getCheckItem(String checkMasterId, String checkDate, String startDate, String endDate) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("checkMasterId", checkMasterId);  
		paramMap.addValue("checkDate", checkDate);  
		paramMap.addValue("startDate", startDate);  
		paramMap.addValue("endDate", endDate);  
		
		String sql = """
				select ci.id
	            , c."Name" as check_master_name
	            , ci."ItemGroup1" as group1
	            , ci."ItemGroup2" as group2
	            , ci."ItemGroup3" as group3
	            , ci."Name" as item_name
	            , ci."Code" as item_code 
	            , ci."CycleType" as cycle_type 
	            , ci."CycleValue" as cycle_value
	            , ci."ResultType" as result_type
	            , ci."StartDate" as start_date
	            , ci."EndDate" as end_date
	            , ci."_order" as index_order
	            , ci."minValue" as min_value
	            , ci."maxValue" as max_value
	            from check_item ci
	            inner join check_mast c on c.id = ci."CheckMaster_id"
	            where 1=1
				""";
		
		if (!checkMasterId.isEmpty()) sql += " and c.id = cast(:checkMasterId as Integer) ";
		if(checkDate != null && !checkDate.isEmpty()) {
			sql += " and ci.\"StartDate\" <= cast(:checkDate as date) and ci.\"EndDate\" >= cast(:checkDate as date) ";
		} else {
			sql += " and ci.\"StartDate\" <= cast(:startDate as date) and ci.\"EndDate\" >= cast(:endDate as date) ";
		}
		
		sql += " order by ci._order asc, C.\"Name\" desc ";
				
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
	}

	public Map<String, Object> getCheckItemDetail(Integer id) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("id", id);
		
		String sql = """
				select  ci.id
	            , ci."CheckMaster_id" as check_master_id
	            , ci."ItemGroup1" as group1
	            , ci."ItemGroup2" as group2
	            , ci."ItemGroup3" as group3
	            , ci."Name" as item_name
	            , ci."Code" as item_code 
	            , ci."CycleType" as cycle_type
	            , ci."CycleValue" as cycle_value
	            , ci."ResultType" as result_type
	            , ci."StartDate" as start_date
	            , ci."EndDate" as end_date
	            , af.id as file_id
	            , af."FileName" as "fileName"
	            , ci."minValue" as min_value
	            , ci."maxValue" as max_value
	            from check_item ci
	            inner join check_mast c on c.id = ci."CheckMaster_id"
	            left join attach_file af on af."DataPk" = ci.id  and af."TableName" ='check_item'
	            where ci.id = :id
				""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);

        return items;
	}

	public Map<String, Object> getImageDetailCheckItem(Integer dataPk) {
		
		MapSqlParameterSource paramMap = new MapSqlParameterSource();        
		paramMap.addValue("dataPk", dataPk);
		
		String sql = """
				select id as file_id , "DataPk" as "dataPk", "FileIndex" as file_index
				, "TableName", "PhysicFileName", "ExtName"
                from attach_file as af
                where af."TableName" = 'check_item'
                and af."DataPk"  = :dataPk
				""";
		
        Map<String, Object> items = this.sqlRunner.getRow(sql, paramMap);

        return items;
	}
	
	public ResponseEntity<Resource> getImageShowCheckItem(Integer dataPk) throws IOException{
		
		Map<String, Object> row = this.getImageDetailCheckItem(dataPk);
		
		String tableName = CommonUtil.tryString(row.get("TableName"));
        String physicFileName = CommonUtil.tryString(row.get("PhysicFileName"));
        String extName = CommonUtil.tryString(row.get("ExtName"));

        String path = settings.getProperty("file_upload_path") + tableName + "/";
        String file_name = physicFileName;
        String content_type = "image/" + extName;

        //File file = new File(path + file_name);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", content_type);
        Path filePath = Paths.get(path + file_name);
        Resource resource = null;
        try {
            resource = new UrlResource(filePath.toUri());
        } catch (MalformedURLException e) {
            e.printStackTrace();
        }

        return ResponseEntity.ok()
                .headers(headers)
                .body(resource);
    }
}
