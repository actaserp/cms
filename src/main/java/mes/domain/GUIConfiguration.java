package mes.domain;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.springframework.core.io.ClassPathResource;
import com.fasterxml.jackson.databind.ObjectMapper;

import mes.domain.model.GUIData;

public class GUIConfiguration {	

	private static final GUIConfiguration configuration = new GUIConfiguration();
	
	private final Map<String, GUIData> guiDataMap = new HashMap<String, GUIData>();
	
	private GUIConfiguration(){
	    this.loadJsonConfig();
	}
	
	public static GUIData getGUIInfo(String gui) {
		return configuration.guiDataMap.get(gui);
	}
	
	public static Map<String, GUIData> getGUIDataMap(){
		return configuration.guiDataMap;
	}
	
	@SuppressWarnings("unchecked")
	private void loadJsonConfig() {
		
		ObjectMapper objectMapper = new ObjectMapper();
		
		ClassPathResource resource = new ClassPathResource("gui.json");
		InputStreamReader inputStreamReader;
		try {
			inputStreamReader = new InputStreamReader(resource.getInputStream(), "UTF-8");
			
			Stream<String> streamOfString= new BufferedReader(inputStreamReader).lines();
	        String json = streamOfString.collect(Collectors.joining());	
	        
	        
			Map<String, Object> map = objectMapper.readValue(json, Map.class);		
	        
			for( Map.Entry<String, Object> elem : map.entrySet() ){
				String guiCode = (String)elem.getKey();
				Map<String, Object> dicInfo = (Map<String, Object>)elem.getValue();
				String menuName = (String)dicInfo.get("name");
				
				Map<String, String> template = (Map<String, String>)dicInfo.get("templates");
				
				GUIData guiData = new GUIData(menuName, template);
				
				this.guiDataMap.put(guiCode, guiData);
				
	        }	        
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
}
