package mes.app.haccp;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.haccp.service.MetalDetectMService;
import mes.domain.entity.MetalDetectM;
import mes.domain.entity.MetalDetectMDetail;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MetalDetectMDetailRepository;
import mes.domain.repository.MetalDetectMRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/haccp/metal_detect_m")
public class MetalDetectMController {

	@Autowired
	private MetalDetectMService metalDetectMService;
	
	@Autowired
	MetalDetectMRepository metalDetectMRepository;
	
	@Autowired
	MetalDetectMDetailRepository metalDetectMDetailRepository;
	
	@GetMapping("/read")
	public AjaxResult getMetalDetectM() {
		
        List<Map<String, Object>> items = this.metalDetectMService.getMetalDetectM();      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/info")
	public AjaxResult getMetalDetectMInfo(@RequestParam("id") int id) {
		
		Map<String, Object> items = this.metalDetectMService.getMetalDetectMInfo(id);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveMetalDetectM(
			@RequestParam(value = "id", required = false) Integer id,
			@RequestParam(value = "Code", required = false) String code,
			@RequestParam(value = "Type", required = false) String type,
			@RequestParam(value = "Description", required = false) String description,
			@RequestParam(value = "Name", required = false) String name,
			@RequestParam(value = "ProductionTestCycle", required = false) String productionTestCycle,
			@RequestParam(value = "TestCount", required = false) String testCount,
			@RequestParam(value = "TestPiece", required = false) String testPiece,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		MetalDetectM md = null;
		
		if (id == null) {
			md = new MetalDetectM();
		} else {
			md = this.metalDetectMRepository.getMetalDetectMById(id);
		}
		Integer count = Integer.parseInt(testCount);
		md.setCode(code);
		md.setType(type);
		md.setName(name);
		md.setTestCount(count);
		md.setProductionTestCycle(Integer.parseInt(productionTestCycle));
		md.setTestPiece(testPiece);
		md.setDescription(description);
		md.set_audit(user);
		
		md = this.metalDetectMRepository.save(md);
		
		Integer dataPk = md.getId();
		
		List<MetalDetectMDetail> mddList = this.metalDetectMDetailRepository.findByMetalDetectMasterId(dataPk);
		
		if (mddList.size() > 0) {
			this.metalDetectMDetailRepository.deleteByMetalDetectMasterId(mddList.get(0).getMetalDetectMasterId());
		}
		
		for(int i = 1; i <= count; i++) {
			MetalDetectMDetail mdd = this.metalDetectMDetailRepository.findByMetalDetectMasterIdAndOrder(dataPk,i);
			if (mdd == null) {
				mdd = new MetalDetectMDetail();
				mdd.setMetalDetectMasterId(dataPk);
				mdd.setOrder(i);
				mdd.set_audit(user);
				mdd = this.metalDetectMDetailRepository.save(mdd);
			}
		}
		
		return result;
	}
	
	@PostMapping("/delete")
	@Transactional
	public AjaxResult deleteMetalDetect(
			@RequestParam(value = "id", required = false) Integer id) {
		
		AjaxResult result = new AjaxResult();
		
		if(id != null) {
			this.metalDetectMRepository.deleteById(id);
			this.metalDetectMDetailRepository.deleteByMetalDetectMasterId(id);
		}
		
		return result;
	}
	
	@GetMapping("/detail_list")
	public AjaxResult detailList(
			@RequestParam(value = "master_id", required = false) Integer masterId) {
		
        List<Map<String, Object>> items = this.metalDetectMService.detailList(masterId);
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@PostMapping("/save_detail_list")
	@Transactional
	public AjaxResult saveDetailList(
			@RequestBody MultiValueMap<String,Object> Q,
			@RequestParam(value = "master_id", required = false) Integer masterId,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

	    List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());

	    for (int i = 0; i < data.size(); i++) {
	    	Integer id = Integer.parseInt(data.get(i).get("id").toString());
	    	String testTarget = data.get(i).get("test_target") != null ? data.get(i).get("test_target").toString() : null;
	    	String PiecePosition1 = data.get(i).get("piece_position1") != null ? data.get(i).get("piece_position1").toString() : null;
	    	String PiecePosition2 = data.get(i).get("piece_position2")!= null ? data.get(i).get("piece_position2").toString() : null;
	    	
	    	MetalDetectMDetail md = this.metalDetectMDetailRepository.getMetalDetectMDetailById(id);
	    	
	    	md.setOrder(i + 1);
	    	md.setTestTarget(testTarget);
	    	md.setPiecePosition1(PiecePosition1);
	    	md.setPiecePosition2(PiecePosition2);
	    	md.set_audit(user);
	    	
	    	md = this.metalDetectMDetailRepository.save(md);
	    }
	    
	    return result;
	    
	}
}
