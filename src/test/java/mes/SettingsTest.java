package mes;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.junit.jupiter.api.Test;
import mes.config.Settings;


@SpringBootTest
public class SettingsTest {

	
	@Autowired
	Settings settings;
	
	@Test
	public void propertyTest() {
		
		String host = this.settings.getProperty("mqtt_host");
		System.out.println(host);
		
		
    	String jdbc_url = this.settings.getProperty("spring.datasource.hikari.jdbc-url");
    	String username = this.settings.getProperty("spring.datasource.hikari.username");
    	String password = this.settings.getProperty("spring.datasource.hikari.password");
    	
    	//String conn = "jdbc:postgresql://10.10.10.231:5432/bread_mes?user=mes21&password=mes7033&ssl=false";
    	String conn = String.format("%s?user=%s&password=%s&ssl=false", jdbc_url, username, password);
		System.out.println(conn);
	}
	
}
