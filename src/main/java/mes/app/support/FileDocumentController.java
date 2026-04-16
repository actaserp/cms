package mes.app.support;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.codehaus.jettison.json.JSONException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.support.service.FileDocumentService;
import mes.domain.entity.DocForm;
import mes.domain.entity.DocResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DocFormRepository;
import mes.domain.repository.DocResultRepository;

@RestController
@RequestMapping("/api/support/file_document")
public class FileDocumentController {

	@Autowired
	private FileDocumentService fileDocumentService;

	@Autowired
	DocFormRepository docFormRepository;

	@Autowired
	DocResultRepository docResultRepository;

	@Autowired
	private FileService fileService;
	
	@GetMapping("/read")
	public AjaxResult getFileDocumentList(
    		@RequestParam(value="doc_form", required=false) String doc_form, 
    		@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to, 
    		@RequestParam(value="keyword", required=false) String keyword,
			HttpServletRequest request) throws JSONException {
		
		Integer doc_form_id = getDocFormId(doc_form);
		
        List<Map<String, Object>> items = this.fileDocumentService.getFileDocumentList(doc_form_id, date_from, date_to, keyword);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getFileDocumentList(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
			
		Map<String, Object> items = this.fileDocumentService.getFileDocumentList(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/getImages")
	public AjaxResult getImages(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="table_name", required=false) String tableName
			) {
		
        List<Map<String, Object>> items = this.fileDocumentService.getImages(id, tableName);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveDocForm(
			@RequestParam(value="doc_form", required=false) String doc_form,
			@RequestParam(value="doc_name", required=false) String doc_name,
			@RequestParam(value="content", required=false) String content,
			@RequestParam(value="doc_date", required=false) String doc_date,
			@RequestParam(value="id", required=false) Integer doc_id,
			@RequestParam(value="fileId", required=false) String file_id,
			HttpServletRequest request,
			Authentication auth) {
        
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		Integer doc_form_id = getDocFormId(doc_form);
		
		DocResult docResult = null;
		
		if (doc_id == null) {
			docResult = new DocResult();		
		} else {
			docResult = this.docResultRepository.getDocResultById(doc_id);
		}

		docResult.setDocumentName(doc_name);
		docResult.setDocumentFormId(doc_form_id);
		docResult.setContent(content);
		docResult.setDocumentDate(Date.valueOf(doc_date));
        docResult.set_audit(user);

		docResult = this.docResultRepository.save(docResult);

		if (file_id != null && !file_id.isEmpty())  {
			
			Integer data_pk = docResult.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(id, data_pk);
			}
		}
		
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteDocForm(@RequestParam("id") Integer id) {
		
		if (id != null) {
			this.docResultRepository.deleteById(id);
		}
		
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	private Integer getDocFormId(String pDocForm) {
		
		Integer rtnFormId = 0;
		
		List<DocForm> docForm = this.docFormRepository.findByFormNameAndFormType(pDocForm, "file");
		
		if (docForm.size() > 0) {
			rtnFormId = docForm.get(0).getId();
		}
		
		return rtnFormId;
	}	
}
