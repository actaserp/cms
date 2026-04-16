package mes.app.system.service;

import java.util.List;
import java.util.Map;

import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

@Mapper
public interface SystemLogMapper {
	public List<Map<String, Object>> getSystemLogList(@Param("start")String start, @Param("end")String end, @Param("type")String type);
	
	public Map<String, Object> getSystemLogDetail(long id);
}
