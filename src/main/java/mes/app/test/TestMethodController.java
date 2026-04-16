package mes.app.test;

import java.util.Optional;

import javax.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.test.service.TestMethodService;
import mes.domain.entity.TestMethod;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TestMethodRepository;

@RestController
@RequestMapping("/api/test/test_method")
public class TestMethodController {


	@Autowired
	TestMethodService testMethodService;
	
	@Autowired
	TestMethodRepository testMethodRepository;
	
	@GetMapping("/read")
	public AjaxResult read(@RequestParam(value="keyword", required=false) String keyword){
		AjaxResult result = new AjaxResult();
		result.data = this.testMethodService.getTestMethodList(keyword);
		return result;
	}
	
	@GetMapping("/detail")
	public AjaxResult detail(@RequestParam(value="id", required=true) int id){
		AjaxResult result = new AjaxResult();
		result.data = this.testMethodService.getTestMethodDetail(id);
		return result;
	}

	@PostMapping("/save")
	public AjaxResult save(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="test_method_code", required=true) String test_method_code,
			@RequestParam(value="test_method_name", required=true) String test_method_name,
			@RequestParam(value="equip_group_id", required=false) Integer equip_group_id,
			@RequestParam(value="description", required=false) String description,
			HttpServletRequest request,
			Authentication auth) {
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		Optional<TestMethod> optTestMethod = this.testMethodRepository.getTestMethodByCode(test_method_code);
		TestMethod testMethod = null;
		
		if (optTestMethod.isPresent()) {
			testMethod = optTestMethod.get();
			if(testMethod.getId()!=id) {
				result.success = false;
				result.message = "이미 존재하는 코드입니다.";
				return result;
			}
		}else {
			if(id!=null) {
				//기존코드값 변경건
				testMethod = this.testMethodRepository.getTestMethodById(id);
				
			}else{
				testMethod = new TestMethod();
			}
		}
		
		testMethod.setName(test_method_name);
		testMethod.setCode(test_method_code);
		testMethod.setEquipmentGroupId(equip_group_id);
		testMethod.setDescription(description);
		testMethod.set_audit(user);
		this.testMethodRepository.save(testMethod);
		
		result.data = testMethod.getId();

		return result;
	}
	
	@GetMapping("/test_item_list")
	public AjaxResult getTestItemList(@RequestParam(value="id", required=true) int id) {
		AjaxResult result = new AjaxResult();
		result.data = this.testMethodService.getTestItemList(id);
		return result;
	}
	
	@PostMapping("/delete")
	public AjaxResult deleteTestMethod(@RequestParam(value="id", required=true) int id) {
		AjaxResult result = new AjaxResult();
		this.testMethodRepository.deleteById(id);
		return result;
	}
	
	
}
