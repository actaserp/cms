package mes.app.test;

import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.test.service.TestItemService;
import mes.domain.entity.TestItem;
import mes.domain.entity.TestResultCode;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TestItemRepository;
import mes.domain.repository.TestResultCodeRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/test/test_item")
public class TestItemController {
	
	@Autowired
	private TestItemService testItemService;
	
	@Autowired
	TestItemRepository testItemRepository;

	@Autowired
	TestResultCodeRepository testResultCodeRepository;
	
	// 목록 조회
	@GetMapping("/read")
	public AjaxResult getDocFormList(
			@RequestParam(value="test_method_id", required=false) String testMethodId,
			@RequestParam(value="test_item_name", required=false) String testItemName,
			@RequestParam(value="test_res_type", required=false) String testResType,
			@RequestParam(value="unit", required=false) Integer unit,
			HttpServletRequest request) {

		List<Map<String, Object>> items = this.testItemService.getTestItemList(testMethodId, testItemName, testResType, unit);
		
        AjaxResult result = new AjaxResult();
        result.data = items;
		return result;
	}
	
	// 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getTestItemDetailItem(
			@RequestParam("id") Integer id, 
			HttpServletRequest request) {
        Map<String, Object> items = this.testItemService.getTestItemDetailItem(id);
        
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}

	// 선택형결과 상세 정보 조회
	@GetMapping("/detail_res")
	public AjaxResult getTestItemDetailRes(
			@RequestParam("id") Integer testItemId, 
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.testItemService.getTestItemDetailRes(testItemId);
        
        AjaxResult result = new AjaxResult();
        result.data = items;        
		return result;
	}
	
	// 정보 저장
	@PostMapping("/save")
	public AjaxResult saveTestItem(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="res_type", required=false) String resType,
			@RequestParam(value="item_code", required=false) String itemCode,
			@RequestParam(value="item_name", required=false) String itemName,
			@RequestParam(value="item_type", required=false) String itemType,
			@RequestParam(value="item_eng_name", required=false) String itemEngName,
			@RequestParam(value="item_unit", required=false) Integer itemUnit,
			@RequestParam(value="item_round_digit", required=false) Integer itemRoundDigit,
			@RequestParam(value="test_method_id", required=false) Integer testMethodId,
			HttpServletRequest request,
			Authentication auth) {

        AjaxResult result = new AjaxResult();
		User user = (User)auth.getPrincipal();
		
		TestItem testItem = null;
		
		if (id == null) {	// 추가
			testItem = new TestItem();
		} else {			// 수정
			testItem = this.testItemRepository.getTestItemById(id);
		}
		
		// 데이터 체크
		boolean codeChk = this.testItemRepository.findByCode(itemCode).isEmpty();	// 코드

		if (itemCode.equals(testItem.getCode()) == false && codeChk == false) {
			result.success = false;
			result.message="이미 존재하는 코드입니다.";
			return result;
		}
		
		boolean nameChk = this.testItemRepository.findByName(itemName).isEmpty();	// 항목명
		
		if (itemName.equals(testItem.getName()) == false && nameChk == false) {
			result.success = false;
			result.message="이미 존재하는 항목명입니다.";
			return result;
		}
		
		testItem.setResultType(resType);
		testItem.setCode(itemCode);
		testItem.setName(itemName);
		testItem.setEngName(itemEngName);
		testItem.setItemType(itemType);
		testItem.setUnitId(CommonUtil.tryIntNull(itemUnit));
		testItem.setRoundDigit(CommonUtil.tryIntNull(itemRoundDigit));
		testItem.setTestMethodId(CommonUtil.tryIntNull(testMethodId));
		testItem.set_audit(user);
		
		testItem = this.testItemRepository.save(testItem);		
        result.data = testItem;
		
		return result;
	}

	// 상세 정보 삭제
	@PostMapping("/delete")
	public AjaxResult deleteTestItem(@RequestParam("id") Integer id) {
		this.testItemRepository.deleteById(id);
		AjaxResult result = new AjaxResult();
		return result;
	}
	
	// 선택형결과 정보 저장
	@PostMapping("/save_res")
	public AjaxResult saveRes(
			@RequestParam(value="test_item_id", required=true) int testItemId,
			@RequestParam MultiValueMap<String,Object> saveList,
			HttpServletRequest request,
			Authentication auth) {
		
        AjaxResult result = new AjaxResult();
        
		User user = (User)auth.getPrincipal();
		List<Map<String, Object>> items = CommonUtil.loadJsonListMap(saveList.getFirst("list").toString());
			
		for (int i = 0; i < items.size(); i++) {
			Map<String, Object> item = items.get(i);
			
			Integer id = (Integer)item.get("id");
			int test_item_id = testItemId;
			String result_code = (String) item.get("result_code");
			String result_name = (String) item.get("result_name");
			String pass_yn = (String) item.get("pass_yn");
			
			TestResultCode testResultCode = null;
		
			if (id == null) {
				testResultCode = new TestResultCode();
			} else {
				testResultCode = this.testResultCodeRepository.getTestResultItemById(id);
			}
			
			boolean codeChk = this.testResultCodeRepository.findByTestItemIdAndResultCode(testItemId, result_code).isEmpty();
						
			if (result_code.equals(testResultCode.getResultCode()) == false && codeChk == false){
				result.success = false;
				result.message="이미 존재하는 코드입니다.";
				return result;
			}
			
			if (id == null) {
				testResultCode.setTestItemId(test_item_id);
			}
			testResultCode.setResultCode(result_code);
			testResultCode.setResultName(result_name);
			testResultCode.setPassYn(pass_yn);
			testResultCode.set_audit(user);
			
			testResultCode = this.testResultCodeRepository.save(testResultCode);		
	        result.data = testResultCode;
			
		}
		return result;
	}

    // 선택형결과 정보 삭제
	@PostMapping("/delete_res")
	public AjaxResult deleteRes(@RequestParam("id") Integer id) {
		if (id != null) {
			this.testResultCodeRepository.deleteById(id);
		}
		AjaxResult result = new AjaxResult();
		return result;
	}
}
