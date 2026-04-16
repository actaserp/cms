package mes.domain.model;

import java.util.Map;
public class GUIData {

	public String name;
	public Map<String, String> templates;	
	
	public GUIData(String menuName, Map<String, String> tmpl) {
		this.name= menuName;
		this.templates = tmpl;		
	}
}
