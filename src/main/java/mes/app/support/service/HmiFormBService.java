package mes.app.support.service;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;
import io.micrometer.core.instrument.util.StringUtils;
import mes.config.Settings;
import mes.domain.services.SqlRunner;

@Service
public class HmiFormBService {

	@Autowired
	SqlRunner sqlRunner;	
	
	@Autowired
	Settings settings;

	// HMI양식B 리스트 조회
	public List<Map<String, Object>> getHmiBList(String keyword){
		MapSqlParameterSource dicParam = new MapSqlParameterSource(); 
		dicParam.addValue("keyword", keyword);

        String sql = """
        		select id
                , "FormName" as form_name
                , to_char("StartDate", 'yyyy-mm-dd') || ' ~ ' || to_char("EndDate", 'yyyy-mm-dd') as apply_date
                , to_char(_created, 'yyyy-mm-dd') as created_date
                , "Description" as description
                from doc_form
                where "FormType" = 'hmi_b'
            """;
        if (StringUtils.isEmpty(keyword)==false) 
        	sql += " and \"FormName\" ilike concat('%%',upper( :keyword ),'%%') ";

        sql += " order by _created desc ";
        		
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, dicParam);

        return items;
	}
	
	// HMI양식B 리스트 상세조회
	public Map<String, Object> getHmiBDetail(Integer form_id){
		
		MapSqlParameterSource dicParam = new MapSqlParameterSource();        
        dicParam.addValue("form_id", form_id);
        
        String sql = """
        		select id
	            , "FormName" 
	            , "Content"
	            , "Description" 
	            from doc_form
	            where id = :form_id
		    """;
        
        Map<String, Object> item = this.sqlRunner.getRow(sql, dicParam);
        
        return item;
	}

	public List<Map<String, Object>> getImageList() {
		
		String imgPath = settings.getProperty("hmi_node_image_path");
		
		String fileName = "";
		
		File rw = new File(imgPath);
		
		File[] fileList = rw.listFiles();
		
		List<Map<String,Object>> item = new ArrayList<Map<String,Object>>();
		
		
		for(File file : fileList) {
		      if(file.isFile()) {
		         fileName = file.getName();
		         Map<String,Object> name = new HashMap<String,Object>();
		         name.put("value", fileName);
		         name.put("text", fileName);
		         
		         item.add(name);
		      }
		}
		
		return item;
	}
	
	public List<Map<String, Object>> getBackgroundImageList() {
		
		String imgPath = settings.getProperty("hmi_background_image_path");
		
		String fileName = "";
		
		File rw = new File(imgPath);
		
		File[] fileList = rw.listFiles();
		
		List<Map<String,Object>> item = new ArrayList<Map<String,Object>>();
		
		
		for(File file : fileList) {
		      if(file.isFile()) {
		         fileName = file.getName();
		         Map<String,Object> name = new HashMap<String,Object>();
		         name.put("value", fileName);
		         name.put("text", fileName);
		         
		         item.add(name);
		      }
		}
		
		return item;
	}
}
