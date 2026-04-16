package mes.app.haccp;

import java.sql.Date;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;


import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.common.service.FileService;
import mes.app.haccp.service.HaccpStandardService;
import mes.domain.entity.BundleHead;
import mes.domain.entity.DocResult;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.BundleHeadRepository;
import mes.domain.repository.DocResultRepository;

@RestController
@RequestMapping("/api/haccp/haccp_standard")
public class HaccpStandardController {

	@Autowired
	private HaccpStandardService haccpStandardService;

	@Autowired
	private FileService fileService;

	@Autowired
	DocResultRepository docResultRepository;
	
	@Autowired
	BundleHeadRepository bundleHeadRepository;
	
	@GetMapping("/read_list")
	public AjaxResult getStandardRead(
			@RequestParam(value="start_date", required=false) String startDate, 
			@RequestParam(value="end_date", required=false) String endDate,
			HttpServletRequest request) {
		
		List<Map<String,Object>> items = this.haccpStandardService.getStandardRead(startDate,endDate);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
		
	
	@GetMapping("/result_list")
	public AjaxResult getResultList(
			@RequestParam(value="bh_id", required=false) Integer bhId,
			HttpServletRequest request) {
		
		Map<String, Object> items = this.haccpStandardService.getResultList(bhId);
		AjaxResult result = new AjaxResult();
		result.data = items;
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getDocumentDetailList(
    		@RequestParam(value="form_id", required=false) Integer id,
			HttpServletRequest request) {
			
		Map<String, Object> items = this.haccpStandardService.getDocumentDetailList(id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 저장
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveDocForm(
			@RequestParam(value="bh_id", required=false, defaultValue= "0") Integer bhId,
			@RequestParam(value="doc_id", required=false, defaultValue= "0") Integer docId,
			@RequestParam(value="check_date", required=false) String check_date,
			@RequestParam(value="title", required=false) String title,
			@RequestParam(value="cboDocForm", required=false) Integer cboDocForm,
			@RequestParam(value="content", required=false) String content,
			@RequestParam(value="doc_date", required=false) String docDate,
			@RequestParam(value="doc_name", required=false) String docName,
			@RequestParam(value="fild_id", required=false) String file_id,
			HttpServletRequest request,
			Authentication auth) {
        
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		Timestamp checkDate = Timestamp.valueOf(check_date+ " 00:00:00");
		
		BundleHead bh = new BundleHead();
		
		if(bhId > 0) {
			bh = this.bundleHeadRepository.getBundleHeadById(bhId);
		} else {
			bh.setTableName("haccp_standard_doc");
		}
		
		bh.setChar1(title);
		bh.setDate1(checkDate);
		bh.set_audit(user);
		
		this.bundleHeadRepository.save(bh);
		
		DocResult docResult = new DocResult();
		
		if (docId > 0) {
			docResult = this.docResultRepository.getDocResultById(docId);
		}

		docResult.setDocumentName(docName);
		docResult.setText1("bundle_head");
		docResult.setNumber1((float)bh.getId());
		docResult.setDocumentFormId(cboDocForm);
		docResult.setContent(content);
		docResult.setDocumentDate(Date.valueOf(docDate));
		docResult.set_audit(user);
		
		docResult = this.docResultRepository.save(docResult);
		
		if (file_id != null && file_id != "") {
			
			Integer data_pk = bh.getId();
			String[] fileIdList = file_id.split(",");
			
			for (String fileId : fileIdList) {
				int id = Integer.parseInt(fileId);
				this.fileService.updateDataPk(id, data_pk);
			}
		}
		
		Map<String,Object> item = new HashMap<>();
		item.put("id", bh.getId());
		
		result.data = item;
		
		return result;
	}
	
	// 삭제
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteDocForm(@RequestParam("bh_id") Integer bh_id) {
		
		this.bundleHeadRepository.deleteById(bh_id);
		
		List<DocResult> drList = this.docResultRepository.findByNumber1AndText1((float)bh_id, "bundleHead");
		
		for(int i = 0; i < drList.size(); i++) {
			this.docResultRepository.deleteById(drList.get(i).getId());
		}
		AjaxResult result = new AjaxResult();
		return result;
	}
	
}
