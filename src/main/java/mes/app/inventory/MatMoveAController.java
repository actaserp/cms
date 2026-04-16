package mes.app.inventory;

import java.text.ParseException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
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

import mes.app.inventory.service.MatMoveAService;
import mes.domain.entity.MaterialInout;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MatInoutRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/inventory/mat_move_a")
public class MatMoveAController {

	@Autowired
	private MatMoveAService matMoveAService;

	@Autowired
	MatInoutRepository matInoutRepository;

	// 창고이동 조회
	@GetMapping("/read")
	public AjaxResult getMatMoveList(
			@RequestParam(value="storehouse", required=false) Integer storehouse, 
    		@RequestParam(value="mat_group", required=false) Integer mat_group,
    		@RequestParam(value="material", required=false) Integer material,
    		@RequestParam(value="mat_name", required=false) String mat_name,
			HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.matMoveAService.getMatMoveList(storehouse, mat_group, material, mat_name);      
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 창고 이동처리
	@PostMapping("/move")
	@Transactional
	public AjaxResult moveInoutQty(
			@RequestParam(value = "to_storehouse_id", required = false) Integer to_storehouse_id,
			@RequestBody MultiValueMap<String,Object> move_list,
			HttpServletRequest request,
			Authentication auth) throws ParseException {

		AjaxResult result = new AjaxResult();

		User user = (User)auth.getPrincipal();
		
	    List<Map<String, Object>> items = CommonUtil.loadJsonListMap(move_list.getFirst("move_list").toString());

		if (items.size() == 0) {
			result.success = false;
			return result;
		}

		LocalDateTime localDateTime = LocalDateTime.now();
		
		LocalDate inout_date = localDateTime.toLocalDate();
		LocalTime inout_time = localDateTime.toLocalTime();
		
		for (int i = 0; i < items.size(); i++) {
		
			Float inout_qty = Float.valueOf((String) items.get(i).get("move_qty"));
			Integer mat_id = Integer.valueOf((Integer) items.get(i).get("mat_id"));
			Integer store_house_id = Integer.valueOf((Integer) items.get(i).get("store_house_id"));
			
            // 이동출고
			MaterialInout mat_out = new MaterialInout();
			mat_out.setInoutDate(inout_date);
            mat_out.setInoutTime(inout_time);
            mat_out.setInOut ("out");
            mat_out.setOutputType("move_out");
            mat_out.setOutputQty(inout_qty);
            mat_out.setMaterialId(mat_id);
            mat_out.setStoreHouseId(store_house_id);
            mat_out.setState("confirmed");
            mat_out.set_status("a");
            mat_out.set_audit(user);
            mat_out = this.matInoutRepository.save(mat_out);
    		
            this.matInoutRepository.flush();
            
            Integer matoutId = mat_out.getId();

            // 이동입고
            MaterialInout mat_in =  new MaterialInout();
            mat_in.setInoutDate(inout_date);
            mat_in.setInoutTime(inout_time);
            mat_in.setInOut("in");
            mat_in.setInputType("move_in");
            mat_in.setInputQty(inout_qty);
            mat_in.setMaterialId(mat_id);
            mat_in.setStoreHouseId(to_storehouse_id);
            mat_in.setState("confirmed");
            mat_in.set_status("a");
            mat_in.setSourceDataPk(matoutId);     
            mat_in.setSourceTableName("mat_inout");
            mat_in.set_audit(user);
            mat_in = this.matInoutRepository.save(mat_in);
            
            this.matInoutRepository.flush();            
		}
		
		return result;
	}
}
