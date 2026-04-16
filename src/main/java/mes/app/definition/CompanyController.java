package mes.app.definition;


import java.sql.Date;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

import javax.servlet.http.HttpServletRequest;

import mes.app.naverCloud.Enum.NcpMetric;
import mes.app.naverCloud.dto.DataQueryRequest;
import mes.app.naverCloud.service.NcpMonitoringService;
import mes.app.naverCloud.strategy.MonthlyRange;
import mes.app.naverCloud.strategy.RealTimeRange;
import org.apache.groovy.parser.antlr4.util.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.*;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.util.MultiValueMap;

import mes.app.definition.service.CompanyService;
import mes.app.definition.service.PriceService;
import mes.domain.entity.Company;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CompanyRepository;
import org.springframework.web.client.RestTemplate;

@RestController
@RequestMapping("/api/definition/company")
public class CompanyController {

	@Autowired
	CompanyRepository companyRepository;
	
	@Autowired
	private CompanyService companyService;
	
	@Autowired
	private PriceService priceService;

    @Autowired
    private NcpMonitoringService ncpMonitoringService;

	
	/**
	 * @apiNote 업체조회
	 * @param compType 업체타입
	 * @param groupName 그룹이름
	 * @param keyword 키워드
	 */
	@GetMapping("/read")
	public AjaxResult getCompanyList(
			@RequestParam("comp_type") String compType,
			/*@RequestParam("group_name") String groupName,*/
			@RequestParam("keyword") String keyword,
			@RequestParam(value ="spjangcd") String spjangcd) {
		
		List<Map<String, Object>> items = this.companyService.getCompnayList(compType, keyword, spjangcd);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}




	// 업체 상세정보 조회
	@GetMapping("/detail")
	public AjaxResult getCompnayDetail(
			@RequestParam("company_id") int companyId,
			HttpServletRequest request) {
		Map<String, Object> item = this.companyService.getCompanyDetail(companyId);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}

	// 업체 정보 저장
	@PostMapping("/save")
	public AjaxResult saveCompany(
			//기본정보 탭
			@RequestParam(value="id", required=false) Integer id,
			@RequestParam("name") String name,
			@RequestParam("company_type") String CompanyType,
			@RequestParam("eng_name") String EngName,
			@RequestParam("comp_code") String compCode,
			@RequestParam("comp_code2") String compCode2,
			@RequestParam("business_number") String businessNumber,
			@RequestParam("tel_number") String telNumber,
			@RequestParam("fax_number") String faxNumber,
			@RequestParam("business_type") String businessType,
			@RequestParam("ceo_name") String ceoName,
			@RequestParam("email") String email,
			@RequestParam("homepage") String homepage,
			@RequestParam("business_item") String businessItem,
			@RequestParam(value="group_name" , required=false) String groupName,
			@RequestParam("zip_code") String zipCode,
			@RequestParam("address") String address,
			@RequestParam("description") String description,
			@RequestParam("relyn") String relyn,
			@RequestParam(value = "IsPersonal" , required = false) String IsPersonal,
			@RequestParam("CorporateNumber") String CorporateNumber,
			@RequestParam("PrintTradingName") String PrintTradingName,
			//관리 정보
			@RequestParam("our_manager") String ourManager,
			@RequestParam("sales_manager") String salesManager,
			@RequestParam("first_trading_day") String firstTradingDay,
			@RequestParam("purchase_sales_deadline") String purchaseSalesDeadline,
			@RequestParam("account_manager") String accountManager,
			@RequestParam("account_manager_phone") String accountManagerPhone,
			@RequestParam("last_trading_day") String lastTradingDay,
			@RequestParam(value = "receivable_amount", required = false) String receivableAmount,
			@RequestParam(value = "unpaid_amount", required = false) String unpaidAmount,
			/*@RequestParam("credit_limit_amount") String creditLimitAmount,*/
			@RequestParam("tranding_bank") String trandingBank,
			@RequestParam("account_holder") String accountHolder,
			@RequestParam("account_number") String accountNumber,
			@RequestParam("payment_condition") String paymentCondition,
			@RequestParam("manage_remark") String manageRemark,
			@RequestParam(value ="spjangcd") String spjangcd,
			HttpServletRequest request,
			Authentication auth ) {
		User user = (User)auth.getPrincipal();
		
		Company company = null;
		
		if (id==null) {
			company = new Company();
		}else {
			company = this.companyRepository.getCompnayById(id);
		}
		//기본정보
		company.setName(name);
		company.setCompanyType(CompanyType);
		company.setEngName(EngName);
		company.setCode(compCode);
		company.setCode2(compCode2);
		company.setBusinessNumber(businessNumber);
		company.setTelNumber(telNumber);
		company.setFaxNumber(faxNumber);
		company.setBusinessType(businessType);
		company.setCEOName(ceoName);
		company.setEmail(email);
		company.setHomePage(homepage);
		company.setBusinessItem(businessItem);
		company.setGroupName(groupName);
		company.setZipCode(zipCode);
		company.setAddress(address);
		company.setDescription(description);
		company.setRelyn(String.valueOf("1".equals(relyn) ? 1 : 0));
		company.setIsPersonal("1".equals(IsPersonal) ? "1" : "0");
		company.setCorporateNumber(CorporateNumber);
		company.setPrintTradingName(PrintTradingName);
		//관리정보
		company.setOurManager(ourManager);
		company.setSalesManager(salesManager);
		company.setFirstTradingDay(!StringUtils.isEmpty(firstTradingDay) ? Date.valueOf(firstTradingDay) : null);
		company.setPurchaseSalesDeadline(purchaseSalesDeadline);
		company.setAccountManager(accountManager);
		company.setAccountManagerPhone(accountManagerPhone);
		company.setLastTradingDay(!StringUtils.isEmpty(lastTradingDay) ?Date.valueOf(lastTradingDay) : null);
		company.setReceivableAmount(!StringUtils.isEmpty(receivableAmount) ? Float.valueOf(receivableAmount) : null);
		company.setUnpaidAmount(!StringUtils.isEmpty(unpaidAmount) ? Float.valueOf(unpaidAmount) : null);
		/*company.setCreditLimitAmount(!StringUtils.isEmpty(creditLimitAmount) ? Float.valueOf(creditLimitAmount) : null);*/
		company.setTrandingBank(trandingBank);
		company.setAccountHolder(accountHolder);
		company.setAccountNumber(accountNumber);
		company.setPaymentCondition(paymentCondition);
		company.setManageRemark(manageRemark);
		company.set_audit(user);
		company.setSpjangcd(spjangcd);

		company = this.companyRepository.save(company);
		
		AjaxResult result = new AjaxResult();
        result.data=company;
		return result;
	}
	
	//업체 정보 삭제
	@PostMapping("/delete")
	public AjaxResult deleteCompnay(@RequestParam("id") Integer id) {
		
		this.companyRepository.deleteById(id);
		
		AjaxResult result = new AjaxResult();
		
		return result;
	}
	
	// 업체 단가정보
	@GetMapping("/mat_price_list")
	public AjaxResult getPriceListByCompany(
			@RequestParam("company_id") int companyId,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.companyService.getPriceListByCompany(companyId);
		DateTimeFormatter fmt = DateTimeFormatter.ofPattern("yyyy-MM-dd");
		for (Map<String, Object> item : items) {
			// ApplyStartDate 포맷 변환
			Object startDate = item.get("ApplyStartDate");
			if (startDate != null) {
				// 예: java.sql.Timestamp 또는 String인 경우 처리
				LocalDate localDate = null;
				if (startDate instanceof java.sql.Timestamp) {
					localDate = ((java.sql.Timestamp) startDate).toLocalDateTime().toLocalDate();
				} else if (startDate instanceof String) {
					localDate = LocalDate.parse(((String) startDate).substring(0,10));
				}
				item.put("ApplyStartDate", localDate.format(fmt));
			}

			Object endDate = item.get("ApplyEndDate");
			if (endDate != null) {
				LocalDate localDate = null;
				if (endDate instanceof java.sql.Timestamp) {
					localDate = ((java.sql.Timestamp) endDate).toLocalDateTime().toLocalDate();
				} else if (endDate instanceof String) {
					localDate = LocalDate.parse(((String) endDate).substring(0,10));
				}
				item.put("ApplyEndDate", localDate.format(fmt));
			}
		}
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
		
	// 단가 상세 조회
	@GetMapping("/detail_price")
	public AjaxResult getMaterialPriceDetail(
			@RequestParam("id") int priceId,
			HttpServletRequest request) {
		Map<String, Object> item = this.companyService.getMaterialPriceDetail(priceId);
		
		AjaxResult result = new AjaxResult();
		result.data = item;
		
		return result;
	}
	
	// 품목별 단가 히스토리 리스트 조회
	@GetMapping("/read_price_history")
	public AjaxResult getPriceHistoryByComp(
			@RequestParam("comp_id") int companyId,
			@RequestParam("code") String Code,
			HttpServletRequest request) {
		List<Map<String, Object>> items = this.companyService.getPriceHistoryByComp(companyId,Code);
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
		
	// 단가 정보 등록/변경 
	@PostMapping("/save_price")
	public AjaxResult matCompUnitPriceSaveNew (@RequestBody MultiValueMap<String, Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        data.set("user_id", user.getId());
        
        AjaxResult result = new AjaxResult();
        
        if (this.priceService.saveCompanyUnitPrice(data) > 0) {
        	
        } else {
        	result.success = false;
        };
        
        return result;
	}
	// 단가 정보 수정 
	@PostMapping("/update_price")
	public AjaxResult updatePriceByMat(@RequestBody MultiValueMap<String, Object> data) {
		SecurityContext sc = SecurityContextHolder.getContext();
        Authentication auth = sc.getAuthentication();         
        User user = (User)auth.getPrincipal();
        data.set("user_id", user.getId());
        
        AjaxResult result = new AjaxResult();
		
        if (this.priceService.updateCompanyUnitPrice(data) > 0) {
        	
        } else {
        	result.success = false;
        }; 
        
		return result;
	} 
	
	// 단가 정보 삭제 
	@PostMapping("/delete_price")
	public AjaxResult deletePriceByMat(@RequestParam("id") int priceId) {
        
        AjaxResult result = new AjaxResult();
		
        if (this.priceService.deleteCompanyUnitPrice(priceId) > 0) {
        	
        } else {
        	result.success = false;
        }; 
        
		return result;
	}
}
