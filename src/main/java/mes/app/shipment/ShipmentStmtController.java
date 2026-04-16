package mes.app.shipment;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;

import lombok.extern.slf4j.Slf4j;
import mes.app.util.UtilClass;
import mes.domain.entity.MatCompUprice;
import mes.domain.repository.MatCompUpriceRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.PageRequest;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import mes.app.shipment.service.ShipmentListService;
import mes.app.shipment.service.ShipmentStmtService;
import mes.app.shipment.service.TradeStmtService;
import mes.domain.entity.Shipment;
import mes.domain.entity.ShipmentHead;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ShipmentHeadRepository;
import mes.domain.repository.ShipmentRepository;
import mes.domain.services.CommonUtil;
import mes.domain.services.DateUtil;

@RestController
@RequestMapping("/api/shipment/shipment_stmt")
@Slf4j
public class ShipmentStmtController {

	@Autowired
	private ShipmentListService shipmentListService;
	
	@Autowired
	private ShipmentStmtService shipmentStmtService;

	@Autowired
	private MatCompUpriceRepository matCompUpriceRepository;

	@Autowired
	ShipmentHeadRepository shipmentHeadRepository;

	@Autowired
	ShipmentRepository shipmentRepository;

	@Autowired
	TradeStmtService tradeStmtService;

	// 출하지시 목록 조회
	@GetMapping("/order_list")
	public AjaxResult getOrderList(
			@RequestParam(value="srchStartDt", required=false) String date_from,
			@RequestParam(value="srchEndDt", required=false) String date_to,
			@RequestParam(value="cboCompany", required=false) String comp_pk,
			@RequestParam(value="cboMatGroup", required=false) String mat_grp_pk,
			@RequestParam(value="cboMaterial", required=false) String mat_pk) {
		
		List<Map<String, Object>> items = this.shipmentListService.getShipmentHeadList(date_from, date_to, comp_pk, mat_grp_pk, mat_pk, null, "");
		
		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}
	
	// 출하 품목
	@GetMapping("/shipment_item_list")
	public AjaxResult getShipmentItemList(
			@RequestParam(value="head_id", required=false) String head_id,
			@RequestParam(value="company_id", required=false) Integer company_id,
			@RequestParam(value="calc_money", required=false) String calc_money) {
		
		List<Map<String, Object>> items = null;
		AjaxResult result = new AjaxResult();


		items = this.shipmentListService.getShipmentItemList(head_id, company_id);

		result.success = true;
		result.data = items;
		
		return result;
	}
	
	// 단가 변경
	@PostMapping("/update_unit_price")
	@Transactional
	public AjaxResult saveUnitPrice(
			@RequestParam(value="head_id", required=false) Integer head_id,
			@RequestBody MultiValueMap<String,Object> dataList,
			HttpServletRequest request,
			Authentication auth) {
		
		User user = (User)auth.getPrincipal();
		
		AjaxResult result = new AjaxResult();
		
		List<Map<String, Object>> item = CommonUtil.loadJsonListMap(dataList.getFirst("Q").toString());

		if (item.size() == 0) {
			result.success = false;
			return result;
		}
		
		ShipmentHead head = this.shipmentHeadRepository.getShipmentHeadById(head_id);
		
		if (head == null) {			
			result.success = false;
			return result;			
		} else {
			
			if ("Y".equals(head.getStatementIssuedYN())) {
				result.success = false;
				return result;				
			} else {				
				double price_sum = 0.0;
				double vat_sum = 0.0;
				List<MatCompUprice> matCompUpriceList = new ArrayList<>();

				for (int i = 0; i < item.size(); i++) {
					
					Integer ship_pk = (Integer) item.get(i).get("ship_pk");
					Shipment shipment = this.shipmentRepository.getShipmentById(ship_pk);
					
					if (shipment != null) {
						
						/*String vat_exempt_yn = (String) item.get(i).get("vat_exempt_yn");
						
						if (vat_exempt_yn == null || vat_exempt_yn == "") {
							vat_exempt_yn = "N";
						}*/
			            
						Double unit_price = CommonUtil.tryDoubleNull(item.get(i).get("unit_price"));
						Double order_qty = shipment.getOrderQty();
						String invatyn = (String) item.get(i).get("invatyn");

						Integer material_id = UtilClass.getInt(item.get(i), "material_id");
						Integer company_id = UtilClass.getInt(item.get(i), "company_id");

						Boolean flag = Boolean.TRUE.equals(item.get(i).get("flag"));

						double price = 0;
						double vat = 0;

						if(invatyn.equals("N")){
							price = unit_price * order_qty;
							vat = price * 0.1;

							price_sum += price;
							vat_sum += vat;

						}else{
							double vat_exam_price = (unit_price * ((double) 10 /11));

							price = vat_exam_price * order_qty;
							vat = (unit_price * 0.1) * order_qty;

							price_sum += price;
							vat_sum += vat;

						}
						shipment.setUnitPrice(unit_price);
						shipment.setPrice(price);
						shipment.setVat(vat);

						MatCompUprice matCompUprice = new MatCompUprice();
						//unit_price
						if(flag){
							//flag가 true면 mat_comp_uprice에 이력이 없음 --> 신규추가
							matCompUprice.set_created(Timestamp.from(ZonedDateTime.now().toInstant()));

							matCompUprice.set_creater_id(user.getId());
							matCompUprice.setUnitPrice(unit_price);
							matCompUprice.setFormerUnitPrice(null);
							matCompUprice.setApplyStartDate(Timestamp.from(ZonedDateTime.now().toInstant()));
							matCompUprice.setApplyEndDate(
									Timestamp.from(ZonedDateTime.of(LocalDateTime.of(2100, 12, 31, 0, 0), ZoneId.of("Asia/Seoul")).toInstant())
							);
							matCompUprice.setChangeDate(Timestamp.from(ZonedDateTime.now().toInstant()));
							matCompUprice.setChangerName("관리자");
							matCompUprice.setCompanyId(company_id);
							matCompUprice.setMaterialId(material_id);
							matCompUprice.setType("02");

							matCompUpriceList.add(matCompUprice);

						}else{
							//이력이 있다는 뜻
							MatCompUprice matCompUprice1 = matCompUpriceRepository.findLastestOne(company_id, material_id, PageRequest.of(0, 1))
									.stream()
									.findFirst()
									.orElse(null);

							Double formerUnitPrice =  matCompUprice1.getUnitPrice();

							matCompUprice1.setUnitPrice(unit_price);
							matCompUprice1.setFormerUnitPrice(formerUnitPrice);
							matCompUprice1.setChangeDate(Timestamp.from(ZonedDateTime.now().toInstant()));

							matCompUpriceList.add(matCompUprice1);
						}

						matCompUpriceRepository.saveAll(matCompUpriceList);

	                    shipment.setUnitPrice(unit_price.doubleValue());
	                    shipment.setPrice(price);
	                    shipment.setVat(vat);
	                    shipment.set_audit(user);
	                    
	                    this.shipmentRepository.save(shipment);
					}
				}

                head.setTotalPrice(price_sum);
                head.setTotalVat(vat_sum);
                head.set_audit(user);
                
                this.shipmentHeadRepository.save(head);
			}
		}
		
		return result;	
	}
		
	// 명세서 발행처리	
	@PostMapping("/update_stmt_issue")
	public AjaxResult issueStatement(
			@RequestParam("head_id") Integer head_id,
			HttpServletRequest request,
			Authentication auth) {
		User user = (User)auth.getPrincipal();
		AjaxResult result = new AjaxResult();		
		ShipmentHead head = this.shipmentHeadRepository.getShipmentHeadById(head_id);
		if (head == null) {
			result.success = false;
			return result;
		} else {
			if ("Y".equals(head.getStatementIssuedYN())) {
				result.success = false;
				return result;
			} else {

                head.setStatementIssuedYN("Y");
                head.setIssueDate(DateUtil.getNowTimeStamp());	//DateUtil.getTodayString()
                head.setStatementNumber("");
                head.set_audit(user);
                head = this.shipmentHeadRepository.save(head);
                result.data = head;
			}
		}
		return result;
	}

	// 거래명세서 출력
	@PostMapping("/print_trade_stmt")
	public AjaxResult printTradingStatement(
			@RequestParam(value="head_id", required=false) Integer head_id,
			@RequestParam(value="company_id") Integer company_id
			) {


		Map<String, Object> header = this.tradeStmtService.getTradeStmtHeaderInfo(head_id);
		List<Map<String, Object>> items = this.tradeStmtService.getTradeStmtItemList(head_id, company_id);
		
        Map<String, Object> rtnData = new HashMap<String, Object>();
        rtnData.putAll(header);
        rtnData.put("item_list", items);

		AjaxResult result = new AjaxResult();
		result.data = rtnData;
		return result;
	}
}
