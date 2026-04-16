package mes.domain.services;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class CommonUtil {
	
	public static float tryFloat(Object data) {		
		float result;
		
		if (data==null) {
			return 0;
		}		
		
		try {			
			result = (float)data;			
		} catch (Exception e) {
			result = 0;
		}
		return result;
	}
	
	public static float tryFloat(String data) {		
		float result;
		
		if (StringUtils.hasText(data)==false) {
			return 0;
		}		
		
		try {			
			result = Float.parseFloat(data);			
		} catch (Exception e) {
			result = 0;
		}
		return result;
	}
	
	public static double tryDouble(Object data) {		
		double result;
		
		if (data==null) {
			return 0;
		}		
		
		try {			
			result = (double)data;			
		} catch (Exception e) {
			result = 0;
		}
		return result;
	}

	public static int tryInt(Object data) {
		int result;
		try {
			result =  (int)data;
		} catch (Exception e) {
			result = 0;
		}
		return result;
	}
	
	public static int tryInt(String data) {
		int result;
		try {
			result =  Integer.parseInt(data);
		} catch (Exception e) {
			result = 0;
		}
		return result;
	}	
	
	public static Float tryFloatNull(Object data) {
		Float result;
		try {
			result = Float.valueOf(data.toString());
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	public static Integer tryIntNull(Object data) {
		Integer result;
		try {
			result = Integer.valueOf(data.toString());
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	public static Double tryDoubleNull(Object data) {
		Double result;
		try {
			result = Double.valueOf(data.toString());
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	public static String tryString(Object data) {
		String result;
		try {
			result = data.toString();
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	public static Timestamp tryTimestamp(Object data) {
		Timestamp result;
		String dateVal = (data.toString()).length() == 10 ? data.toString() + " 00:00:00" : data.toString();

		try {
			result = Timestamp.valueOf(dateVal);
		} catch (Exception e) {
			result = null;
		}
		return result;
	}
	
	public static Date trySqlDate(String date) {
		Date convertDate;
		try {
			convertDate = Date.valueOf(date);
		} catch(Exception e) {
			convertDate = null;
		}

		return convertDate;
	}	
	
	public static Map<String, Object> loadJsonToMap(String strJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		Map<String, Object> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<Map<String,Object>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static List<Map<String, Object>> loadJsonListMap(String strJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		List<Map<String, Object>> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<Map<String,Object>>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static List<List<Map<String, Object>>> loadJsonMulipleListMap(String strJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		List<List<Map<String, Object>>> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<List<Map<String, Object>>>>() {});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return result;
	}
	
	public static List<Integer> loadJsonListInteger(String strJson) {
		ObjectMapper objectMapper = new ObjectMapper();
		List<Integer> result = null;
		try {
			result = objectMapper.readValue(strJson, new TypeReference<List<Integer>>(){});
		} catch (JsonMappingException e) {
			e.printStackTrace();
		} catch (JsonProcessingException e) {
			e.printStackTrace();
		}
		
		return result;
	}	
	
	public static String getUtf8FileName(String filename) {
		return URLEncoder.encode(filename, StandardCharsets.UTF_8);
	}
}
