package mes;

import java.util.List;

import org.junit.Assert;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import mes.app.common.service.FileService;
import mes.domain.entity.AttachFile;
import mes.domain.repository.AttachFileRepository;

@SpringBootTest
public class AttachFileServiceTest {
	
	
	@Autowired
	AttachFileRepository attachFileRepository;
	
	
	@Autowired
	FileService attachFileService;

	
	@Test
	public void getAttachFileTest() {
		
		String tableName = "doc_result";
		Integer dataPk =12;
		String attachName = "basic";

		
		AttachFile attFile = this.attachFileService.getAttachFileByData(tableName, dataPk, attachName);
		
		Assert.assertTrue(attFile!=null);
	}
	

	@Test
	public void getAttachFileRepoTest() {
		
		String tableName = "xxxxxxx";
		Integer dataPk =3;
		String attachName = "basic";

		
		List<AttachFile> list = this.attachFileRepository.getAttachFileByTableNameAndDataPkAndAttachName(tableName, dataPk, attachName);
		
		Assert.assertTrue(list!=null);
		System.out.println(list.size());
	}


}
