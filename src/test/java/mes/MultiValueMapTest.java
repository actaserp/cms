package mes;


import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import mes.domain.services.CommonUtil;

import static org.junit.Assert.assertEquals;

@SpringBootTest
public class MultiValueMapTest {

	@Test
	void getSetTest() {
		MultiValueMap<String,Object> data = new LinkedMultiValueMap<>();
		
		data.set("user_id", 3);
		
		System.out.println("user_id 1 : "+data.get("user_id"));
		System.out.println("user_id 2 : "+data.getFirst("user_id"));
		System.out.println("user_id 3 : "+CommonUtil.tryIntNull(data.getFirst("user_id").toString()));
	}
	
	@Test
	void typeCastingTest() {
		String dateVal = "2022-08-08";
		
		System.out.println("dataVal len : "+dateVal.length());
		//System.out.println("date value 2 : "+(Timestamp.valueOf(dateVal)).toString());
		System.out.println("date value 1 : "+(CommonUtil.tryTimestamp(dateVal)).toString());
		
	}
}
