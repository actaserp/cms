package mes.app.definition;

import java.sql.Date;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import mes.domain.repository.EquComponentRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;

import mes.app.common.service.FileService;
import mes.app.definition.service.EquipmentService;
import mes.domain.entity.Equipment;
import mes.domain.entity.EquipmentHistory;
import mes.domain.entity.PropData;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.EquipmentHistoryRepository;
import mes.domain.repository.EquipmentRepository;
import mes.domain.repository.PropDataRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/definition/equipment")
public class EquipmentController {
	
	@Autowired
	private EquipmentService equipmentService;
	
	@Autowired
	EquipmentRepository equipmentRepository;
	
	@Autowired
	private FileService fileService;
	
	@Autowired
	PropDataRepository propDataRepository;
	
	@Autowired
	EquipmentHistoryRepository equipmentHistoryRepository;

	@Autowired
	EquComponentRepository equComponentRepository;
	
	// 설비 목록 조회
	@GetMapping("/read")
	public AjaxResult getEquipmentList(
			@RequestParam(value="group", required=false) Integer group,
			@RequestParam(value="workcenter", required=false) Integer workcenter,
			@RequestParam(value="equipment", required=false) String keyword,
    		HttpServletRequest request) {
       
        List<Map<String, Object>> items = this.equipmentService.getEquipmentList(group, workcenter, keyword);      
        
        AjaxResult result = new AjaxResult();
        result.data = items;        				
        
		return result;
	}
	
	// 설비 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getEquipmentpDetail(
			@RequestParam("id") int id, 
    		HttpServletRequest request) {
        Map<String, Object> item = this.equipmentService.getEquipmentpDetail(id);      
               		
        AjaxResult result = new AjaxResult();
        result.data = item;        				
        
		return result;
	}
	
	//설비 저장
	@PostMapping("/save")
	@Transactional
	public AjaxResult saveEquipment(
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam(value="EquipmentGroup_id", required=false) Integer EquGrp_id,
			@RequestParam(value="Code") String Code,
			@RequestParam(value="Name") String Name,
			@RequestParam(value="ManageNumber", required=false) String ManageNumber,
			@RequestParam(value="WorkCenter_id") Integer WorkCenter_id,
			@RequestParam(value="Model", required=false) String Model,
			@RequestParam(value="Maker", required=false) String Maker,
			@RequestParam(value="SerialNumber", required=false) String SerialNumber,
			@RequestParam(value="ProductionYear", required=false) Integer ProductionYear,
			@RequestParam(value="PurchaseDate", required=false) String PurchaseDate,
			@RequestParam(value="InstallDate", required=false) String InstallDate,
			@RequestParam(value="SupplierName", required=false) String SupplierName,
			@RequestParam(value="PurchaseCost", required=false) Float PurchaseCost,
			@RequestParam(value="OperationRateYN", required=false) String OperationRateYN,
			@RequestParam(value="DisposalDate", required=false) String DisposalDate,
			@RequestParam(value="Manager", required=false) String Manager,
			@RequestParam(value="Description", required=false) String Description,
			@RequestParam(value="Voltage", required=false) String Voltage,
			@RequestParam(value="PowerWatt", required=false) Integer PowerWatt,
			@RequestParam(value="InputDate", required=false) String InputDate,
			@RequestParam(value="Depart_id", required=false) Integer Depart_id,
			@RequestParam(value="Usage", required=false) String Usage,
			@RequestParam(value="ASTelNumber", required=false) String ASTelNumber,
			@RequestParam(value="AttentionRemark", required=false) String AttentionRemark,
			@RequestParam(value="OvenProductTemperStandard", required=false) Double OvenProductTemperStandard,
			@RequestParam(value="OvenHeatingMnStandard", required=false) Double OvenHeatingMnStandard,
			@RequestParam(value="Located", required=false) String Located,
			//@RequestParam(value="UnusedChk", required=false) String UnusedChk,
			//@RequestParam(value="UnusedDate", required=false) Double UnusedDate,
			@RequestParam MultiValueMap<String,Object> Q,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		User user = (User)auth.getPrincipal();
		Equipment equip = null;
		String his_content = "";
		String status = "";
		boolean code_chk = true;//this.equipmentRepository.findByCode(Code).isEmpty();
		boolean name_chk = true;//this.equipmentRepository.findByName(Name).isEmpty();
		
		System.out.println(code_chk);
		
		if (id != null) {
			System.out.println("update");
			his_content = "수정";
			status = "A";
			equip = this.equipmentRepository.getEquipmentById(id);
			
			code_chk = this.equipmentRepository.findByCodeAndIdNot(Code, id).isEmpty();
			name_chk = this.equipmentRepository.findByNameAndIdNot(Name, id).isEmpty();
			
		} else {
			System.out.println("insert");
			his_content = "등록";
			status = "A";
			equip = new Equipment();
			
			code_chk = this.equipmentRepository.findByCode(Code).isEmpty();
			name_chk = this.equipmentRepository.findByName(Name).isEmpty();
		}
		
		if (Name.equals(equip.getName())==false && name_chk == false) {
			result.success = false;
			result.message="중복된 설비명이 존재합니다.";
			return result;
		}
		if (Code.equals(equip.getCode())==false && code_chk == false) {
			result.success = false;
			result.message="중복된 설비코드가 존재합니다.";
			return result;
		}
		
		
		equip.setEquipmentGroup_id(EquGrp_id);
		equip.setCode(Code);
		equip.setName(Name);
		equip.setManageNumber(ManageNumber);
		equip.setWorkCenter_id(WorkCenter_id);
		equip.setModel(Model);
		equip.setMaker(Maker);
		equip.setSerialNumber(SerialNumber);
		equip.setProductionYear(CommonUtil.tryIntNull(ProductionYear));
		equip.setPurchaseDate(CommonUtil.tryTimestamp(PurchaseDate));
		equip.setInstallDate(CommonUtil.tryTimestamp(InstallDate));
		equip.setSupplierName(SupplierName);
		equip.setPurchaseCost(CommonUtil.tryFloatNull(PurchaseCost));
		equip.setOperationRateYN(OperationRateYN);
		equip.setDisposalDate(CommonUtil.tryTimestamp(DisposalDate));
		equip.setManager(Manager);
		equip.setDescription(Description);
		
		equip.setVoltage(Voltage);
		equip.setPowerWatt(PowerWatt);
		equip.setInputDate(InputDate);
		equip.setDepart_id(Depart_id);
		equip.setUsage(Usage);
		equip.setAstelNumber(ASTelNumber);
		equip.setAttentionRemark(AttentionRemark);
		equip.setOvenProductTemperStandard(OvenProductTemperStandard);
		equip.setOvenHeatingMnStandard(OvenHeatingMnStandard);
		equip.setStatus(status);
		equip.set_audit(user);
		
		System.out.println(equip);
		
		equip = this.equipmentRepository.save(equip);
		
		PropData equip_located = this.propDataRepository.findByDataPkAndTableNameAndCode(equip.getId(),"equ", "located");
		if(equip_located == null) {
			equip_located = new PropData();
			equip_located.setDataPk(equip.getId());
			equip_located.setTableName("equ");
			equip_located.setCode("located");
		}
		equip_located.setChar1(Located);
		equip_located.set_audit(user);
		this.propDataRepository.save(equip_located);
		
		//List<Map<String, Object>> qItems = CommonUtil.loadJsonListMap(Q.getFirst("Q").toString());
		// 설비 등록, 수정 이력 생성
		EquipmentHistory equ_his = new EquipmentHistory();
		equ_his.setEquipmentId(equip.getId());
		equ_his.setText1(CommonUtil.tryString(Q.getFirst("Q")));
		equ_his.setContent(his_content);
		equ_his.setDataDate(Date.valueOf(DateUtil.getTodayString()));
		equ_his.set_status("updateHis");
		equ_his.set_audit(user);
		this.equipmentHistoryRepository.save(equ_his);
		
		result.data = equip;
		return result;
	}
	
	
	// 설비 삭제
	@PostMapping("/delete")
	public AjaxResult deleteEquipment(@RequestParam("id") Integer id) {
		// EquipmentHistory 테이블에서 해당 equipment_id를 가진 레코드 조회
		List<EquipmentHistory> historyList = equipmentHistoryRepository.findByEquipmentId(id);

		// 조회된 EquipmentHistory 레코드 삭제
		if (!historyList.isEmpty()) {
			equipmentHistoryRepository.deleteAll(historyList);
		}

		// Equipment 테이블에서 해당 id를 가진 레코드 삭제
		equipmentRepository.deleteById(id);

		return new AjaxResult();
	}


	// 사진 저장
	@PostMapping("/save_photo")
	public AjaxResult savePhoto(
			@RequestParam("eq_id") Integer eq_id,
			@RequestParam("file_id") String file_id,
			HttpServletRequest request,
			Authentication auth) {
		
		AjaxResult result = new AjaxResult();
		
		if (eq_id!=null) {
			String[] file_id_list = file_id.split(",");
			
			for (String id : file_id_list) {
				int id_int = CommonUtil.tryInt(id);
				this.fileService.updateDataPk(id_int, eq_id);
			}
		}
		return result;
	}

	/**
	 *
	 * @param data 추가설비정보
	 * @return
	 */
	@PostMapping("/save_add")
	public AjaxResult saveAdd(@RequestBody MultiValueMap<String,Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
		Authentication auth = sc.getAuthentication();
		User user = (User)auth.getPrincipal();
		data.set("user_id", user.getId());

		AjaxResult result = new AjaxResult();
		try {

			if (this.equipmentService.saveComponent(data) > 0) {

			} else {
				result.success = false;
			}
		} catch (Exception e) {
			e.printStackTrace();
			result.success = false;
			result.message = "서버 오류: " + e.getMessage();
		}
		return result;
	}

	@GetMapping("/Componentdetail")
	public AjaxResult getComponent(@RequestParam("id") int equPk) {
		Map<String, Object> item = this.equipmentService.getComponent(equPk);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}


	@GetMapping("/readComponent")
	public AjaxResult getComponentList(@RequestParam("equ_pk") int equPk) {

		List<Map<String, Object>> items = this.equipmentService.getComponentListByEqu(equPk);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}


	@PostMapping("/componentdelete")
	public AjaxResult deletePriceByMat(@RequestParam("id") int id) {

		AjaxResult result = new AjaxResult();
		this.equComponentRepository.deleteById(id);
		return result;
	}

}
