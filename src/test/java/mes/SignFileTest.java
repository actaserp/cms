package mes;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.util.StringUtils;

import java.io.File;
import java.io.IOException;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.io.FileUtils;




@SpringBootTest
public class SignFileTest {

	@Test
	public void test() {
		
		String sign = "1,2,3";
		
		StringUtils.split(sign, ",");		
		String fileName = "sign"+System.currentTimeMillis()+".png";
		try {
			FileUtils.writeByteArrayToFile(new File("d:\\"+fileName), Base64.decodeBase64(sign));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
}
