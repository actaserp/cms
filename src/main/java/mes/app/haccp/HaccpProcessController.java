package mes.app.haccp;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.HaccpProcessService;
import mes.domain.entity.HaccpProcess;
import mes.domain.entity.HaccpProcessItem;
import mes.domain.entity.RelationData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.HaccpProcessItemRepository;
import mes.domain.repository.HaccpProcessRepository;
import mes.domain.repository.RelationDataRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/haccp_process")
public class HaccpProcessController {

	@Autowired
	private HaccpProcessService haccpProcessService;

	@Autowired
	HaccpProcessRepository haccpProcessRepository;
	
	@Autowired
	RelationDataRepository relationDataRepository;
	
	@Autowired
	HaccpProcessItemRepository HaccpProcessItemRepository; 
	
	// HACCP 공정 목록 조회
	@GetMapping("/read")
	public AjaxResult getHaccpProcessList(@RequestParam(value = "keyword", required = false) String keyword) {
        
		List<Map<String, Object>> items = this.haccpProcessService.getHaccpProcessList(keyword);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// 기본정보 조회
	@GetMapping("/detail_haccp_process")
	public AjaxResult getHaccpProcessDetail(@RequestParam("hp_id") int hp_id) {

        Map<String, Object> item = this.haccpProcessService.getHaccpProcessDetail(hp_id); // haccp_proc detail
        List<Map<String, Object>> processItems = this.haccpProcessService.getHaccpProcessAndProcessList(hp_id); // haccp_process <==> process       
        List<Map<String, Object>> haccpProcessItems = this.haccpProcessService.getHaccpProcessItemList(hp_id); // haccp_process <==> haccp_process_items

        item.put("haccpProcessItems", haccpProcessItems);
        item.put("processItems", processItems);
        AjaxResult result = new AjaxResult();
        result.data = item;        
		return result;
	}
	
	// 기본정보 저장
	@PostMapping("/save_haccp_process")
	@Transactional
	public AjaxResult saveHaccpProcess(
			@RequestParam(value="hp_id", required=false) Integer hp_id,
			@RequestParam(value="haccp_process_code", required=false) String haccp_process_code,
			@RequestParam(value="haccp_process_name", required=false) String haccp_process_name,
			@RequestParam(value="Description", required=false) String Description,
			@RequestParam(value="MonitoringMethod", required=false) String MonitoringMethod,
			@RequestParam(value="ActionMethod", required=false) String ActionMethod,
			@RequestParam(value="TestCycle", required=false) String TestCycle,
			@RequestParam(value="Standard", required=false) String standard,
			@RequestParam(value="processItems", required=false) String strProcessItems,
			@RequestParam(value="ProcessKind", required=false) String processKind,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		
		HaccpProcess haccpProcess = null;
		
		if (hp_id == null) {
			// 신규데이터
			haccpProcess = new HaccpProcess();
		} else {			
			haccpProcess = this.haccpProcessRepository.getHaccpProcessById(hp_id);
			
			List<RelationData> relaList = this.relationDataRepository.findByDataPk1AndTableName1AndTableName2(hp_id, "haccp_proc", "process");
			if (relaList.size()>0) {
				this.relationDataRepository.deleteByDataPk1AndTableName1AndTableName2(hp_id, "haccp_proc", "process");
				this.relationDataRepository.flush();
			}
		}
		
		boolean chkCode = this.haccpProcessRepository.findByCode(haccp_process_code).isEmpty();		
		if (haccp_process_code.equals(haccpProcess.getCode()) == false && chkCode == false) {
			result.success = false;
			result.message="이미 존재하는 코드입니다.";
			return result;
		}
		
		haccpProcess.setCode(haccp_process_code);
		haccpProcess.setName(haccp_process_name);
		haccpProcess.setDescription(Description);
		haccpProcess.setMonitoringMethod(MonitoringMethod);
		haccpProcess.setActionMethod(ActionMethod);
		haccpProcess.setTestCycle(TestCycle);
		haccpProcess.setProcessKind(processKind);
		haccpProcess.setStandard(standard);
		haccpProcess.set_audit(user);
        
        this.haccpProcessRepository.save(haccpProcess);
		
        if(StringUtils.hasText(strProcessItems)) {
        	
        	List<Integer> processItems = CommonUtil.loadJsonListInteger(strProcessItems);
        	
        	for(Integer processId : processItems) {        		
            	RelationData rela_data = new RelationData();        	
            	rela_data.setTableName1("haccp_proc");
            	rela_data.setDataPk1(haccpProcess.getId());
            	rela_data.setTableName2("process");
            	rela_data.setDataPk2(processId);
            	rela_data.setRelationName("");
            	rela_data.set_audit(user);
            	
            	this.relationDataRepository.save(rela_data);        		
        	}
        	
        }
      
        result.data = haccpProcess;
        
		return result;
	}

	// 기본정보 삭제
	@PostMapping("/delete_haccp_process")
	public AjaxResult deleteHaccpProcess(@RequestParam("hp_id") Integer hp_id) {

		AjaxResult result = new AjaxResult();
		
		List<HaccpProcessItem> hpiList = this.HaccpProcessItemRepository.findByHaccpProcessId(hp_id);
		
		if (hpiList.size() > 0) {
			result.success = false;
			result.message="공정을 참조하고 있는 HACCP 항목이 있습니다.";
			return result;
		}
		
		this.haccpProcessRepository.deleteById(hp_id);
		
		return result;
	}
	
	// HACCP 공정별항목 조회
	@GetMapping("/haccp_item_list")
	public AjaxResult getHaccpProcessItemList(@RequestParam("hp_id") int hp_id) {
		
		List<Map<String, Object>> items = this.haccpProcessService.getHaccpProcessItemList(hp_id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}
	
	// HACCP 공정별항목 저장
	@PostMapping("/save_haccp_item")
	@Transactional
	public AjaxResult saveHaccpItem(
			@RequestParam(value="hp_id", required=false) Integer hp_id,
			@RequestBody MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		
		List<Map<String, Object>> haccp_items = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

		// HACCP 항목 저장 삭제=>기존데이터를 삭제하고 목록에 있는 데이터만 저장
		List<HaccpProcessItem> hpiList = this.HaccpProcessItemRepository.findByHaccpProcessId(hp_id);
		
		for (int i=0; i < hpiList.size(); i++) {
			
			Integer hpiId = hpiList.get(i).getId();
			this.HaccpProcessItemRepository.deleteById(hpiId);
			this.HaccpProcessItemRepository.flush();
		}		

		Integer order = 10;
		for (int j=0; j < haccp_items.size(); j++) {			
			
			Integer item_id = haccp_items.get(j).get("item_id") != null ? (Integer) haccp_items.get(j).get("item_id") : null;
			
			if (item_id != null) {
				
				HaccpProcessItem hpitem = new HaccpProcessItem();

                hpitem.setHaccpProcessId(hp_id);
                hpitem.setHaccpItemId(item_id);
                hpitem.setOrder(order);
                hpitem.set_audit(user);
                
                this.HaccpProcessItemRepository.save(hpitem);
                
                order = order + 10;
			}
		}		
                
		return result;
	}
}
