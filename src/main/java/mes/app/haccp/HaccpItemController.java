package mes.app.haccp;

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

import mes.app.haccp.service.HaccpItemService;
import mes.domain.entity.HaccpItem;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.HaccpItemRepository;

@RestController
@RequestMapping("/api/haccp/haccp_item")
public class HaccpItemController {

	@Autowired
	private HaccpItemService haccpItemService;

	@Autowired
	HaccpItemRepository haccpItemRepository;
	
	// 조회
	@GetMapping("/read")
	public AjaxResult getHaccpItemList(
			@RequestParam(value = "keyword", required = false) String keyword,
			HttpServletRequest request) {
        
		List<Map<String, Object>> items = this.haccpItemService.getHaccpItemList(keyword);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}
	
	// 상세 조회
	@GetMapping("/detail")
	public AjaxResult getHaccpItemDetail(
			@RequestParam("item_id") int item_id, 
			HttpServletRequest request) {
		
        Map<String, Object> items = this.haccpItemService.getHaccpItemDetail(item_id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;
        
		return result;
	}
	
	// 저장
	@PostMapping("/save_item")
	public AjaxResult saveItem(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="item_code", required=false) String item_code,
			@RequestParam(value="item_name", required=false) String item_name,
			@RequestParam(value="ResultType", required=false) String ResultType,
			@RequestParam(value="Unit_id", required=false) Integer Unit_id,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		HaccpItem haccpItem = null;
		
		if (id == null) {
			haccpItem = new HaccpItem();
		} else {
			haccpItem = this.haccpItemRepository.getHaccpItemById(id);
		}
		
		boolean chkCode = this.haccpItemRepository.findByCode(item_code).isEmpty();
		
		if (item_code.equals(haccpItem.getName()) == false && chkCode == false) {
			result.success = false;
			result.message="이미 존재하는 코드입니다.";
			return result;
		}

		haccpItem.setCode(item_code);
		haccpItem.setName(item_name);
		haccpItem.setResultType(ResultType);
		haccpItem.setUnitId(Unit_id);
        haccpItem.set_audit(user);
		
        haccpItem = this.haccpItemRepository.save(haccpItem);
	
        result.data = haccpItem;
		
		return result;
	}

	// 삭제
	@PostMapping("/delete_item")
	public AjaxResult deleteItem(@RequestParam("id") Integer id) {
		
		this.haccpItemRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
}
