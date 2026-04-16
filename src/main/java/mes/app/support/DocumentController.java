package mes.app.support;

import java.sql.Timestamp;
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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonMappingException;

import mes.app.common.service.FileService;
import mes.app.support.service.DocumentService;
import mes.domain.entity.Document;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.DocumentRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/support/document")
public class DocumentController {
	
	@Autowired
	private DocumentService documentService;
	
	@Autowired
	DocumentRepository documentRepository;

	@Autowired
	private FileService fileService;
	
	
	/**
	 * 문서 목록 조회
	 * @param formId
	 * @param keyword 키워드
	 * @return
	 * @throws JsonProcessingException 
	 * @throws JsonMappingException 
	 * @throws JSONException 
	 */
	@GetMapping("/read")
	public AjaxResult getDocumentList(
			@RequestParam("form_id") String formId,
			@RequestParam("keyword") String keyword) throws JSONException {
		
		List<Map<String, Object>> items = this.documentService.getDocumentList(formId, keyword);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 문서 목록 상세 조회
	 * @param id
	 * @param request
	 * @return
	 */
	@GetMapping("/detail")
	public AjaxResult getDocumentDetail(
			@RequestParam("id") int id,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.documentService.getDocumentDetail(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	/**
	 * 문서 저장
	 * @param id
	 * @param doc_form_id
	 * @param doc_name
	 * @param content
	 * @param doc_date
	 * @return
	 */
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveDocument(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("doc_form_id") Integer doc_form_id,
			@RequestParam("doc_name") String doc_name,
			@RequestParam("content") String content,
			@RequestParam(value="fileId", required=false) String fileId,
			@RequestParam("doc_date") String doc_date,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		Document doc = null;
		
		if (id==null) {
			doc = new Document();
		} else {
			doc = this.documentRepository.getDocumentById(id);
		}
		Timestamp docDate = CommonUtil.tryTimestamp(doc_date);
		
		doc.setDocumentForm_id(doc_form_id);
		doc.setDocumentName(doc_name);
		doc.setContent(content);
		doc.setDocumentDate(docDate);
		doc.set_audit(user);
		doc = this.documentRepository.save(doc);
		
		if (!fileId.isEmpty()) {
			
			Integer data_pk = doc.getId();
			String[] fileIdList = fileId.split(",");
			
			for (String index : fileIdList) {
				int fId = Integer.parseInt(index);
				this.fileService.updateDataPk(fId, data_pk);
			}
		}
		
		AjaxResult result = new AjaxResult();
        result.data = doc;
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteDocument(@RequestParam("id") Integer id) {
		this.documentRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
