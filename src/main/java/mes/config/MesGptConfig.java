package mes.config;

import mes.app.analysis.util.TableSpecLoader;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.Map;

@Configuration
public class MesGptConfig {

    //@Bean
    public Map<String, List<Map<String, String>>> gptMesTableSpec(TableSpecLoader loader) {
        return loader.loadFromExcel("templates/_tablelist/DataBaseExcel.xlsx");
    }
}

