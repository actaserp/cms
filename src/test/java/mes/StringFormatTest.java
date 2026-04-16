package mes;

import java.sql.Timestamp;
import java.util.Date;


import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StringUtils;

import mes.domain.services.DateUtil;

@SpringBootTest
public class StringFormatTest {

	@Test
	public void formatTest() {
		
		String temp = String.format("%d", 1111111111);		
		System.out.println(temp);
		
		// 천단위 ==>1,111,111,111
		temp = String.format("%,d", 1111111111);		
		System.out.println(temp);
		
		// left padding 0 ==>0011
		temp = String.format("%04d", 11);		
		System.out.println(temp);
		
		// float
		temp = String.format("%f", 3.4);		
		System.out.println(temp); //3.400000
		
		temp = String.format("%.2f", 3.4);		
		System.out.println(temp);//3.40
		
		//반올림 주의
		temp = String.format("%15.6f", 3.123456789);		
		System.out.println(temp);//       3.123457		
		
        //string 스페이스포함10자리
		temp = String.format("%10s", "abcde"); 		
		System.out.println(temp);		
		
		// date ==>2022-08-11
		Date date = new Date();
		temp = String.format("%tF", date); 		
		System.out.println(temp);	
		
		// year ==>2022, %ty는 20
		temp = String.format("%tY", date); 		
		System.out.println(temp);
		
		// 8월
		temp = String.format("%tB", date); 		
		System.out.println(temp);
		
		//오전/오후
		temp = String.format("%tp", date); 		
		System.out.println(temp);

		// 11:35:47
		temp = String.format("%tT", date); 		
		System.out.println(temp);		
	}
	
	@Test
	public void testStringUtils() {
		
		String temp ="";
		System.out.println(StringUtils.hasText(temp)); //false
		
		temp = null;
		System.out.println(StringUtils.hasText(temp)); //false
		
		temp = " ";
		System.out.println(StringUtils.hasText(temp)); //false
		
		temp = "a";
		System.out.println(StringUtils.hasText(temp)); //true		
	}
	
	@Test
	public void testTimestamp() {
		
		Timestamp yesterday = DateUtil.getYesterdayTimestamp(); 		
		System.out.println(yesterday);		
	}
	
	
}
