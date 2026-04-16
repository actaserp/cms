package mes.app.definition;

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

import mes.app.definition.service.UnitService;
import mes.domain.entity.Unit;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.UnitRepository;

@RestController
@RequestMapping("/api/definition/unit")
public class UnitController {

	
	@Autowired
	UnitRepository unitRepository;
	
	@Autowired
	private UnitService unitService;
	
	// 단위 목록 조회
	@GetMapping("/read")
	public AjaxResult getUnitList(@RequestParam("unit_name") String unitName,
								  @RequestParam(value ="spjangcd") String spjangcd,
								  HttpServletRequest request) {
		
        List<Map<String, Object>> items = this.unitService.getUnitList(unitName,spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 단위 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getUnitDetail(
			@RequestParam("unit_id") int unitId, 
    		HttpServletRequest request) {
		
		Optional<Unit> optUnit = this.unitRepository.findById(unitId);
		
        //Map<String, Object> item = this.unitService.getUnitDetail(unitId);
				
		Unit unit = null;
		if(optUnit.isPresent()) {
			unit = optUnit.get();
		}

        AjaxResult result = new AjaxResult();
        result.data = unit;
		return result;
	}
	
	// 단위 정보 저장
	@PostMapping("/save")
	public AjaxResult saveUnit( 
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("name") String name,
			@RequestParam("description") String description,
			@RequestParam("pieceYN") String pieceYN,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth	) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		
		
		Unit unit = null;
		
		if(id ==null) {
			unit = new Unit();
		}else {
			unit = this.unitRepository.getUnitById(id);
		}
		
		boolean nameChk = this.unitRepository.findByName(name).isEmpty();
		
		if(name.equals(unit.getName()) == false && nameChk == false) {
			result.success = false;
			result.message = "중복된 이름이 존재합니다.";
			return result;
		}
		
		unit.setName(name);
		unit.setDescription(description);
		unit.setPieceYN(pieceYN);
		unit.set_audit(user);
		unit.setSpjangcd(spjangcd);

		
		unit = this.unitRepository.save(unit);
		
        result.data=unit;
		return result;
	}

	// 단위 정보 삭제
	@PostMapping("/delete")
	public AjaxResult deleteUnit(@RequestParam("id") Integer id) {
		
		this.unitRepository.deleteById(id);
		
		AjaxResult result = new AjaxResult();
		
		return result;
	}
}
