package mes.app.definition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.definition.service.material.BomByMatService;
import mes.app.definition.service.material.MaterialService;
import mes.app.definition.service.material.RoutingByMatService;
import mes.app.definition.service.material.TestByMatService;
import mes.app.definition.service.material.UnitPriceService;
import mes.domain.entity.Material;
import mes.domain.entity.TestMastMat;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.MaterialRepository;
import mes.domain.repository.TestMastMatRepository;
import mes.domain.services.CommonUtil;

@RestController
@RequestMapping("/api/definition/material")
public class MaterialController {
	
	@Autowired
	private MaterialService materialService;
	
	@Autowired
	private UnitPriceService unitPriceService;
	
	@Autowired
	private BomByMatService bomService;
	
	@Autowired
	private RoutingByMatService routingService;
	
	@Autowired
	private TestByMatService testService;
	
	@Autowired
	MaterialRepository materialRepository;
	
	@Autowired 
	TestMastMatRepository testMastMatRepository;
	
	/**
	 * @apiNote 품목조회
	 * 
	 * @param matType 품목구분
	 * @param matGroupId 품목그룹pk
	 * @param keyword 키워드
	 * @return
	 */
	@GetMapping("/read")
	public AjaxResult getMaterialList(
			@RequestParam("mat_type") String matType, 
    		@RequestParam("mat_group") String matGroupId,
    		@RequestParam("keyword") String keyword,
			@RequestParam(value ="spjangcd") String spjangcd,
			@RequestParam(value ="useYn_flag") String useYnFlag) {
       
        List<Map<String, Object>> items = this.materialService.getMaterialList(matType, matGroupId, keyword,spjangcd, useYnFlag);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote 품목상세조회
	 * 
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/detail")
	public AjaxResult getMaterial(@RequestParam("id") int matPk,
								  @RequestParam(value ="spjangcd") String spjangcd) {
        Map<String, Object> item = this.materialService.getMaterial(matPk,spjangcd);
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}

	@PostMapping("/batchDelete")
	public AjaxResult batchDelete(@RequestBody Map<String, Object> param) {
		List<Integer> ids = (List<Integer>) param.get("ids");
		AjaxResult result = new AjaxResult();
		int count = materialService.deleteMaterials(ids); // 여러개 삭제하는 서비스 메서드 필요
		result.success = count == ids.size();
		result.data = count;
		result.message = result.success ? "삭제 성공" : "삭제 실패";
		return result;
	}

	/**
	 * @apiNote 품목저장(생성/수정)
	 * 
	 * @param data 품목정보
	 * @return
	 */
	@PostMapping("/save")
	public AjaxResult saveMaterial(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        data.set("user_id", user.getId());
        
        AjaxResult result = new AjaxResult();
		
        if (this.materialService.saveMaterial(data) > 0) {
        	
        } else {
        	result.success = false;
			result.message = "등록중 오류가 발생하였습니다.\n사용중지 품목을 확인하여 주십시오";
        }; 
        
		return result;
	}
	
	/**
	 * @apiNote 품목삭제
	 * 
	 * @param matPk 품목pk
	 * @return
	 */
	@PostMapping("/delete")
	public AjaxResult deleteMaterial(@RequestParam("id") int matPk) {
        
        AjaxResult result = new AjaxResult();
		
        if (this.materialService.deleteMaterial(matPk) > 0) {
        	
        } else {
        	result.success = false;
        }; 
        
		return result;
	}
	
	/**
	 * @apiNote 품목 업체별 단가조회
	 * 
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/readPrice")
	public AjaxResult getPriceList(@RequestParam("mat_pk") int matPk) {
       
        List<Map<String, Object>> items = this.unitPriceService.getPriceListByMat(matPk);      
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote 품목 업체별 단가이력 조회
	 * 
	 * @param matPk 품목pk
	 * @return
	 */
	@GetMapping("/readPriceHistory")
	public AjaxResult getPriceHistory(@RequestParam("mat_pk") int matPk,
									  @RequestParam("com_pk") int comPk) {

        List<Map<String, Object>> items = this.unitPriceService.getPriceHistoryByMat(matPk,comPk);
               		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote 품목 업체별 단가상세조회
	 * 
	 * @param pricePk 단가pk
	 * @return
	 */
	@GetMapping("/detailPrice")
	public AjaxResult getPriceDetail(@RequestParam("price_id") int pricePk) {
       
        Map<String, Object> item = this.unitPriceService.getPriceDetail(pricePk);      
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	/**
	 * @apiNote 단가저장(등록/변경)
	 * 
	 * @param data 품목단가정보
	 * @return
	 */
	@PostMapping("/savePrice")
	public AjaxResult savePriceByMat(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        data.set("user_id", user.getId());
        
        AjaxResult result = new AjaxResult();

		try {
			int saveCount = this.unitPriceService.saveCompanyUnitPrice(data);

			if (saveCount > 0) {
				result.success = true;
			} else {
				result.success = false;
				result.message = "저장 실패: 중복된 데이터이거나 입력값이 올바르지 않습니다.";
			}
		} catch (Exception e) {
			result.success = false;
			result.message = "서버 오류: " + e.getMessage();  // 예외 메시지 포함
		}

		return result;
	}
	
	/**
	 * @apiNote 단가수정
	 * 
	 * @param data 품목단가정보
	 * @return
	 */
	@PostMapping("/updatePrice")
	public AjaxResult updatePriceByMat(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        data.set("user_id", user.getId());
        
        AjaxResult result = new AjaxResult();
		
        if (this.unitPriceService.updateCompanyUnitPrice(data) > 0) {
        	
        } else {
        	result.success = false;
        }; 
        
		return result;
	}
	
	/**
	 * @apiNote 단가삭제
	 * 
	 * @param priceId 단가pk
	 * @return
	 */
	@PostMapping("/deletePrice")
	public AjaxResult deletePriceByMat(@RequestParam("id") int priceId) {
        
        AjaxResult result = new AjaxResult();
		
        if (this.unitPriceService.deleteCompanyUnitPrice(priceId) > 0) {
        	
        } else {
        	result.success = false;
        }; 
        
		return result;
	}
	
	/**
	 * @apiNote BOM목록조회
	 * 
	 * @param matPk
	 * @return
	 */
	@GetMapping("/bom")
	public AjaxResult readBomList(@RequestParam("mat_id") String matPk) {
		List<Map<String, Object>> items = this.bomService.getBomListByMat(matPk);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote BOM목록조회
	 * 
	 * @param matPk
	 * @return
	 */
	@GetMapping("/bomReverse")
	public AjaxResult readBomReverseList(@RequestParam("mat_id") int matPk) {
		List<Map<String, Object>> items = this.bomService.getBomReverseListByMat(matPk);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote 라우팅목록조회
	 * 
	 * @param routingPk
	 * @return
	 */
	@GetMapping("/routingProcess")
	public AjaxResult readRoutingProcessList(@RequestParam("routing_pk") String routingPk) {
		List<Map<String, Object>> items = this.routingService.getRoutingProcessList(routingPk);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	/**
	 * @apiNote 검사정보조회
	 * 
	 * @param matPk
	 * @return
	 */
	@GetMapping("/testMaster")
	public AjaxResult readTestMasterList(@RequestParam("mat_id") int matPk) {
		List<Map<String, Object>> items = this.testService.getTestMasterList(matPk);      
   		
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}

	@PostMapping("/save_test_master")
	@Transactional
	public AjaxResult saveTestMaster(
			@RequestParam(value = "mat_id", required = false) Integer matId,
			@RequestParam(value = "InTestYN", required = false) String inTestYN,
			@RequestParam(value = "OutTestYN", required = false) String outTestYN,
			@RequestParam MultiValueMap<String, Object> Q,
			@RequestParam(value = "deletedId", required = false) Integer deletedId, // 삭제할 ID 추가
			HttpServletRequest request,
			Authentication auth) {

		AjaxResult result = new AjaxResult();
		User user = (User) auth.getPrincipal();
		List<TestMastMat> savedData = new ArrayList<>();

		// Material 업데이트
		if (matId != null && matId > 0) {
			Material m = materialRepository.getMaterialById(matId);
			m.setInTestYN(inTestYN);
			m.setOutTestYN(outTestYN);
			m.set_audit(user);
			materialRepository.save(m);
		}

		// 삭제 처리
		if (deletedId != null && deletedId > 0) {
			testMastMatRepository.deleteById(deletedId);
		}

		// 남은 데이터 업데이트
		List<Map<String, Object>> data = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		for (Map<String, Object> item : data) {
			Integer id = item.get("id") != null ? Integer.parseInt(item.get("id").toString()) : null;
			Integer testMasterId = Integer.parseInt(item.get("test_master_id").toString());

			Optional<TestMastMat> existing = testMastMatRepository.findByMaterialIdAndTestMasterId(matId, testMasterId);

			if (existing.isPresent()) {
				TestMastMat tm = existing.get();
				tm.set_audit(user);
				savedData.add(testMastMatRepository.save(tm)); // 수정된 데이터 추가
			} else {
				TestMastMat tm = new TestMastMat();
				tm.setMaterialId(matId);
				tm.setTestMasterId(testMasterId);
				tm.set_audit(user);
				savedData.add(testMastMatRepository.save(tm)); // 새 데이터 추가
			}
		}

		// 최신 데이터 조회 및 반환
		List<TestMastMat> finalData = testMastMatRepository.findByMaterialId(matId);
		result.data = finalData;

		return result;
	}
}
