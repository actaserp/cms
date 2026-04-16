package mes;


import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import mes.domain.GUIConfiguration;
import mes.domain.model.GUIData;


@SpringBootTest
public class GUIConfigTest {
	
		
	@Test
	public void jsonLoadTest(){

		Map<String, GUIData> guiDic = GUIConfiguration.getGUIDataMap();
		
		for( Map.Entry<String, GUIData> elem : guiDic.entrySet() ){
			String guiCode = elem.getKey();
			GUIData guiData = elem.getValue();
			String msg = String.format("%s, %s, %s",guiCode, guiData.name, guiData.templates);
			System.out.println(msg);	
		}		
	}
}