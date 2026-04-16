package mes.app.precedence;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.precedence.service.MatInoutSanitizerService;
import mes.domain.entity.MaterialInout;
import mes.domain.entity.StoreHouse;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatInoutRepository;
import mes.domain.repository.StorehouseRepository;

@RestController
@RequestMapping("/api/precedence/mat_inout_sanitizer")
public class MatInoutSanitizerController {
	
	@Autowired
	public MatInoutSanitizerService matInoutSanitizerService;
	
	@Autowired
	StorehouseRepository storehouseRepository;
	
	@Autowired
	MatInoutRepository matInoutRepository;
	
	@GetMapping("/read")
	public AjaxResult getMatInoutSanitizer(
    		@RequestParam(value="srchStartDt", required=false) String srchStartDt,
    		@RequestParam(value="srchEndDt", required=false) String srchEndDt,
    		@RequestParam(value="keyword", required=false) String keyword) {
		
        List<Map<String, Object>> items = this.matInoutSanitizerService.getMatInoutSanitizer(srchStartDt,srchEndDt,keyword);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult getMatInoutSanitizerDetail(
			@RequestParam(value="mio_pk", required=false) Integer mioPk) {
		
		Map<String, Object> items = this.matInoutSanitizerService.getMatInoutSanitizerDetail(mioPk);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	@PostMapping("/save")
	public AjaxResult saveMatInoutSanitizer(
			@RequestParam(value = "mio_pk", required = false) Integer mioPk,
			@RequestParam(value = "Material_id", required = false) Integer materialId,
			@RequestParam(value = "chxInOut", required = false) String chxInOut,
			@RequestParam(value = "InoutQty", required = false) Integer inoutQty,
			@RequestParam(value = "InoutDate", required = false) String inoutDate,
			@RequestParam(value = "InoutTime", required = false) String inoutTime,
			@RequestParam(value = "Description", required = false) String description,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		LocalDateTime localDateTime = LocalDateTime.now();
		
		LocalDate date = localDateTime.toLocalDate();
		LocalTime time = localDateTime.toLocalTime();
		
		String state = "confirmed";
		String status = "a";
		
		StoreHouse sh = this.storehouseRepository.findByCode("소독제창고");
		
		Integer shPk = null;
		if (sh != null) {
			shPk = sh.getId();
		}
		
		MaterialInout mi = null;
		if( mioPk != null) {
			mi = this.matInoutRepository.getMatInoutById(mioPk);
		} else {
			mi = new MaterialInout();
			
			if (inoutDate != null) {
				mi.setInoutDate(LocalDate.parse(inoutDate));
			} else {
				mi.setInoutDate(date);
			}
			if (inoutTime != null) {
				mi.setInoutTime(LocalTime.parse(inoutTime));
			} else {
				mi.setInoutTime(time);
			}
			
			mi.setMaterialId(materialId);
			mi.setStoreHouseId(shPk);
			mi.setState(state);
			mi.set_status(status);
			mi.set_audit(user);
			
		}
		
		if (chxInOut.equals("chxIn")) {
			mi.setInOut("in");
			mi.setInputType("etc_in");
			mi.setInputQty((float)inoutQty);
		} else {
			mi.setInOut("out");
			mi.setOutputType("etc_out");
			mi.setOutputQty((float)inoutQty);
		}
		mi.setDescription(description);
		mi = this.matInoutRepository.save(mi);
		
		result.data = mi;
		
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteMatInoutSanitizer(
			@RequestParam("mio_pk") Integer mioPk) {
		this.matInoutRepository.deleteById(mioPk);
		AjaxResult result = new AjaxResult();
		return result;
	}

}
