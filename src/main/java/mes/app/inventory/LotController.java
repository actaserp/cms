package mes.app.inventory;

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

import mes.app.inventory.service.LotService;
import mes.domain.entity.MaterialLot;
import mes.domain.entity.MatLotCons;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatLotConsRepository;
import mes.domain.repository.MatLotRepository;

@RestController
@RequestMapping("/api/inventory/lot")
public class LotController {
	
	@Autowired
	private LotService lotService;
	
	@Autowired
	MatLotRepository matLotRepository;
	
	@Autowired
	MatLotConsRepository matLotConsRepository;
	
	
	
	
	// LOT 목록 조회
	@GetMapping("/read")
	public AjaxResult getMatLotList(
			@RequestParam(value="mat_type", required=false) String mat_type, 
			@RequestParam(value="mat_group", required=false) Integer mat_group, 
			@RequestParam(value="material", required=false) Integer material, 
			@RequestParam(value="lot_num", required=false) String lot_num, 
			@RequestParam(value="date_from", required=false) String date_from, 
			@RequestParam(value="date_to", required=false) String date_to, 
			@RequestParam(value="cond", required=false) String cond,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.lotService.getMatLotList(mat_type, mat_group, material, lot_num, date_from, date_to, cond);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// LOT 소비내역 조회
	@GetMapping("/consumed_list")
	public AjaxResult getConsumedList(
			@RequestParam(value="matlot_id", required=false) Integer matlot_id,
			HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.lotService.getConsumedList(matlot_id);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	@PostMapping("/save")
	@Transactional
	public AjaxResult saveMaterialInout(
			@RequestParam("id") Integer id,
			@RequestParam("type") String type,
			@RequestParam("description") String description,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();

		if ("prod".equals(type)) {
			
			MaterialLot ml = this.matLotRepository.getMatLotById(id);
			
			if (ml != null) {
				ml.setDescription(description);
				ml.set_audit(user);
				ml = this.matLotRepository.save(ml);
			} else {
				result.success = false;
			}			
		} else if ("consu".equals(type)) {
			
			MatLotCons ml = this.matLotConsRepository.getMatLotConsById(id);
			
			if (ml != null) {
				ml.setDescription(description);
				ml.set_audit(user);
				ml = this.matLotConsRepository.save(ml);
			} else {
				result.success = false;
			}			
		}
		
		return result;
	}
	
}
