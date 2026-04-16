package mes.config;


import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.core.env.Environment;
import lombok.RequiredArgsConstructor;


@Configuration
@RequiredArgsConstructor
@PropertySource("classpath:application.properties") 
public class Settings {

	private final Environment environment;

    public String getProperty(String key){
        return environment.getProperty(key);
    }   
    
}
