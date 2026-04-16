package mes.app.precedence;

import java.sql.Date;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.ReplaceRepairService;
import mes.domain.entity.MasterResult;
import mes.domain.entity.MasterT;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MasterResultRepository;
import mes.domain.repository.MasterTRepository;

@RestController
@RequestMapping("/api/precedence/place_repair_")
public class ReplaceRepairController {
	
	@Autowired
	private ReplaceRepairService replaceRepairService;
	
	@Autowired
	MasterTRepository masterTRepository; 

	@Autowired
	MasterResultRepository masterResultRepository;

	// 보수내역관리 조회
	@GetMapping("/read_in")
	public AjaxResult getMyFactoryList(
    		@RequestParam(value="date_from", required=false) String date_from, 
    		@RequestParam(value="date_to", required=false) String date_to,
			HttpServletRequest request) {

		Integer master_table_id = getMasterTableId();
		
        List<Map<String, Object>> items = this.replaceRepairService.getMyFactoryList(master_table_id, date_from, date_to);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 보수내역관리 상세조회
	@GetMapping("/detail")
	public AjaxResult getMyFactoryDetailList(
    		@RequestParam(value="id", required=false) Integer id,
			HttpServletRequest request) {
		
		Integer master_table_id = getMasterTableId();
		
        Map<String, Object> items = this.replaceRepairService.getMyFactoryDetailList(master_table_id, id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 저장
	@PostMapping("/save")
	public AjaxResult saveDocForm(
			@RequestParam(value="data_date", required=false) Date data_date,
			@RequestParam(value="worker_count", required=false) Integer worker_count,
			@RequestParam(value="work_content", required=false) String work_content,
			@RequestParam(value="field_tool", required=false) String field_tool,
			@RequestParam(value="work_result", required=false) String work_result,
			@RequestParam(value="fileId", required=false) String file_id,
			@RequestParam(value="work_after_confirm", required=false) String work_after_confirm,
			@RequestParam(value="description", required=false) String Description,
			HttpServletRequest request,
			Authentication auth) {

		Integer master_table_id = getMasterTableId();
		
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		MasterResult mr = new MasterResult();
		
		mr.setDataDate(data_date);
        mr.setMasterTableId(master_table_id);
        mr.setMasterClass("repair_target_place");
        mr.setNumber1(worker_count);
        mr.setChar1(work_content);
        mr.setChar2(field_tool);
        mr.setChar3(work_result);
        mr.setChar4(work_after_confirm);
        mr.setDescription(Description); 
        mr.set_audit(user);
        
        mr = this.masterResultRepository.save(mr);		
		
		return result;
	}
	
	// 삭제
	@PostMapping("/delete")
	public AjaxResult deleteDocForm(@RequestParam("id") Integer id) {
		
		if (id != null) {
			this.masterResultRepository.deleteById(id);
		}
		
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	private Integer getMasterTableId() {
		
		Integer rtnId = 0; 
		Optional<MasterT> masterT = this.masterTRepository.findByCodeAndMasterClass("my_factory", "repair_target_place");
		
		if (masterT.isPresent()) {
			MasterT mt = masterT.get();
			
			rtnId = mt.getId();
		}
		
		return rtnId;
	}
}
