package mes.domain.services;

import java.util.List;
import java.util.Map;

@FunctionalInterface
public interface ComboDataFunction{
	List<Map<String,Object>> getDataList(String cond1, String cond2, String cond3);
}