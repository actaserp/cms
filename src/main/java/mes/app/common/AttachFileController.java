package mes.app.common;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.domain.entity.AttachFile;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AttachFileRepository;
import mes.domain.repository.FileRepository;

@RestController
@RequestMapping("/api/common/attach_file")
public class AttachFileController {
	
	@Autowired
	FileRepository fileRepository;
	
	@Autowired
	AttachFileRepository attachfileRepository;
	
	@Autowired
	private FileService fileService;
	
	@GetMapping("/detailFiles")
	public AjaxResult detailFiles(
			@RequestParam(value="attachName") String attachName,
			@RequestParam(value="TableName", required = true) String TableName,
			@RequestParam(value="DataPk", required = true) Integer DataPk,
			//@RequestParam(value="limit", required = true) Integer limit,
			HttpServletRequest request
			) {
		
		List<Map<String, Object>> items = this.fileService.getAttachFile(TableName, DataPk, attachName);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	//파일 삭제
	@SuppressWarnings("unused")
	@PostMapping("/deleteFile")
	public AjaxResult deleteFile(
			@RequestParam(value="fileId", required = false) Integer id,
			@RequestParam(value="tableName", required = false) String TableName,
			@RequestParam(value="DataPk", required = false) Integer dataPk,
			//@RequestParam(value="physicFileName", required = true) String physicFileName,
			HttpServletRequest request) {
		
		AjaxResult result = new AjaxResult();
		AttachFile af = null;
		//AttachFile q = null;
		
		af = new AttachFile();
		//q = new AttachFile();
		if (dataPk == null) {
  		  af = this.fileRepository.getFileById(id);
		} else {
		  // 리스트에서 선택하여 삭제시 파일도 같이 삭제하기 위해 로직 추가
		  List<AttachFile> dId = this.fileRepository.findByDataPk(dataPk);
		  if (dId.size() > 0) {
			  af = this.fileRepository.getFileById(dId.get(0).getId());
			  this.fileService.deleteByDataPk(dataPk);
		  }
		}
		//String physicFileName = af.getPhysicFileName();
		//Integer dataPk = af.getDataPk();
		
		if (id != null) {
			this.fileRepository.deleteById(id);
			
			//thumbnail 지우기
			// q = this.attachfileRepository.getAttachFileByTableName(TableName);
			// q.filter dataPk, AttachName=thumbnail, PhysicFileName
			// q.delete();
		}
		
		return result;
		
	}

}
