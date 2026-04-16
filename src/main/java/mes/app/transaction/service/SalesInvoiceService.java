package mes.app.transaction.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.popbill.api.*;
import com.popbill.api.taxinvoice.MgtKeyType;
import com.popbill.api.taxinvoice.Taxinvoice;
import com.popbill.api.taxinvoice.TaxinvoiceDetail;
import lombok.extern.slf4j.Slf4j;
import mes.Encryption.EncryptionUtil;
import mes.app.util.UtilClass;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.*;
import mes.domain.services.SqlRunner;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.orm.ObjectOptimisticLockingFailureException;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;


import javax.persistence.OptimisticLockException;
import java.io.FileInputStream;

import java.io.*;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class SalesInvoiceService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private TB_SalesmentRepository tb_salesmentRepository;

    @Autowired
    private TB_SalesDetailRepository tb_salesDetailRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private CloseDownService closeDownService;

    @Autowired
    private TaxinvoiceService taxinvoiceService;

    @Value("${invoice.api.key}")
    private String invoiceeCheckApiKey;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper jacksonObjectMapper;

    @Autowired
    private ShipmentHeadRepository shipmentHeadRepository;

    @Autowired
    private SysCodeRepository sysCodeRepository;

    @Autowired
    Settings settings;

    public List<Map<String, Object>> getList(String invoice_kind, Integer cboStatecode, Integer cboCompany, Timestamp start, Timestamp end, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("invoice_kind", invoice_kind);
        dicParam.addValue("cboStatecode", cboStatecode);
        dicParam.addValue("cboCompany", cboCompany);
        dicParam.addValue("start", start);
        dicParam.addValue("end", end);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                WITH detail_summary AS (
                	SELECT DISTINCT ON (misnum)
                		misnum,
                		itemnm AS first_itemnm,
                		COUNT(*) OVER (PARTITION BY misnum) AS item_count
                	FROM tb_salesdetail
                	ORDER BY misnum, misseq
                )
                  
                  SELECT
                   TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
                   m.misnum,
                   m.misgubun,
                   sale_type_code."Value" AS misgubun_name,  -- fn_code_name 제거
                   m.cltcd,
                   m.ivercorpnum,
                   m.ivercorpnm,
                   m.totalamt,
                   m.supplycost,
                   m.taxtotal,
                   m.statecode,
                   state_code."Value" AS statecode_name,
                   TO_CHAR(TO_TIMESTAMP(m.statedt, 'YYYYMMDDHH24MISS'), 'YYYY-MM-DD HH24:MI:SS') AS statedt_formatted,
                   m.iverceonm,
                   m.iveremail,
                   m.iveraddr,
                   m.taxtype,
                   issue_div."Value" AS issuediv_name,
                   m.issuediv,
                   m.modifycd,
                   CASE
                	   WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
                	   WHEN ds.item_count = 1 THEN ds.first_itemnm
                	   ELSE NULL
                   END AS item_summary
                  
                  FROM tb_salesment m
                  
                  LEFT JOIN tb_salesdetail d
                   ON m.misnum = d.misnum
                  
                  LEFT JOIN detail_summary ds
                   ON m.misnum = ds.misnum
                  
                  LEFT JOIN sys_code sale_type_code
                   ON sale_type_code."CodeType" = 'sale_type'
                   AND sale_type_code."Code" = m.misgubun
                   
                  LEFT JOIN sys_code issue_div
                   ON issue_div."CodeType" = 'issue_div'
                   AND issue_div."Code" = m.issuediv
                  
                  LEFT JOIN sys_code state_code
                   ON state_code."CodeType" = 'state_code_pb'
                   AND state_code."Code" = m.statecode::text
                  
                  WHERE 1 = 1
                  and m.spjangcd = :spjangcd 
                     """; // 조건은 아래에서 붙임

        if (invoice_kind != null && !invoice_kind.isEmpty()) {
            sql += " and m.misgubun = :invoice_kind ";
        }

        if (cboStatecode != null) {
            sql += " and m.statecode = :cboStatecode ";
        }

        if (cboCompany != null) {
            sql += " and m.cltcd = :cboCompany ";
        }

        if (start != null && end != null) {
            sql += " and to_date(m.misdate, 'YYYYMMDD') between :start and :end ";
        }

        sql += """
                GROUP BY
                	m.misdate, m.misnum, m.misgubun, sale_type_code."Value", m.cltcd, m.ivercorpnum,
                	m.ivercorpnm, m.totalamt, m.supplycost, m.taxtotal, m.statecode,
                	state_code."Value", m.statedt, m.iverceonm, m.iveremail,
                	m.iveraddr, m.taxtype, ds.first_itemnm, ds.item_count, issue_div."Value", m.issuediv
                      ORDER BY m.misdate DESC, m.misnum DESC
                      """;

        return this.sqlRunner.getRows(sql, dicParam);
    }

    @Transactional
    public AjaxResult saveInvoicee(Map<String, Object> paramMap, User user) {
        AjaxResult result = new AjaxResult();

        String idStr = (String) paramMap.get("InvoiceeID");
        Integer id = (idStr != null && !idStr.isEmpty()) ? Integer.parseInt(idStr) : null;

        String name = (String) paramMap.get("InvoiceeCorpName");
        String businessNumber = (String) paramMap.get("InvoiceeCorpNum");

        boolean nameExists = (id != null)
                ? companyRepository.existsByNameAndIdNot(name, id)
                : companyRepository.existsByName(name);

        if (nameExists) {
            result.success = false;
            result.message = "이미 등록된 회사명이 존재합니다.";
            return result;
        }

        boolean businessNumberExists = (id != null)
                ? companyRepository.existsByBusinessNumberAndIdNot(businessNumber, id)
                : companyRepository.existsByBusinessNumber(businessNumber);

        if (businessNumberExists) {
            result.success = false;
            result.message = "이미 등록된 사업자등록번호가 존재합니다.";
            return result;
        }

        Company company = (id != null) ? companyRepository.getCompanyById(id) : new Company();

        // 주민번호 암호화
        if (businessNumber.length() == 13) {
            try {
                company.setBusinessNumber(EncryptionUtil.encrypt(businessNumber));
            } catch (Exception e) {
                throw new RuntimeException("암호화 실패", e);
            }

        } else {
            company.setBusinessNumber(businessNumber);
        }

        company.setBusinessNumber(businessNumber);
        company.setAddress((String) paramMap.get("InvoiceeAddr"));
        company.setBusinessItem((String) paramMap.get("InvoiceeBizClass"));
        company.setBusinessType((String) paramMap.get("InvoiceeBizType"));
        company.setCEOName((String) paramMap.get("InvoiceeCEOName"));
        company.setAccountManager((String) paramMap.get("InvoiceeContactName1"));
        company.setName(name);
        company.setInvoiceEmail((String) paramMap.get("InvoiceeEmail1"));
        company.setAccountManagerPhone((String) paramMap.get("InvoiceeTEL1"));
        company.setCompanyType("sale");
        company.set_audit(user);
        company.setRelyn("0");
        company.setSpjangcd((String) paramMap.get("spjangcd"));

        company = companyRepository.save(company);

        if (company.getCode() == null || company.getCode().isEmpty()) {
            company.setCode("Corp-" + company.getId());
            company = companyRepository.save(company);
        }

        result.data = company;
        result.success = true;

        return result;
    }

    @Transactional
    public AjaxResult saveInvoice(@RequestBody Map<String, Object> form) {

        AjaxResult result = new AjaxResult();
        try {

            // 인보이스 저장
            TB_Salesment saved = saveSalesInvoiceInternal(form);

            // 출고 리스트에 저장
            updateShipmentLinks(form, saved);

            result.success = true;

        } catch (ObjectOptimisticLockingFailureException | OptimisticLockException e) {
            result.success = false;
            result.message = "다른 사용자에 의해 데이터가 수정되었습니다. 다시 조회 후 저장해주세요.";
        } catch (Exception e) {
            result.success = false;

            Throwable rootCause = getRootCause(e);
            String rawMessage = rootCause != null ? rootCause.getMessage() : e.getMessage();

            if (rawMessage != null && rawMessage.contains("character varying")) {
                result.message = "입력값이 너무 깁니다. 입력 길이를 확인해주세요.";
            } else {
                result.message = "처리 중 오류가 발생했습니다: " + rawMessage;
            }

            log.error("saveInvoice 예외 발생", e); // 서버 로그에 전체 출력
        }

        return result;
    }

    private TB_Salesment saveSalesInvoiceInternal(Map<String, Object> form) {
        // 1. 기본 키 생성
        Integer misnum = parseInt(form.get("misnum"));
        boolean isUpdate = misnum != null;

        TB_Salesment salesment;

        if (isUpdate) {
            salesment = tb_salesmentRepository.findById(misnum)
                    .orElseThrow(() -> new RuntimeException("해당 misnum의 데이터가 존재하지 않습니다."));

            Integer clientVercode = parseInt(form.get("vercode"));
            if (!Objects.equals(salesment.getVercode(), clientVercode)) {
                throw new OptimisticLockException("버전 불일치: 다른 사용자가 먼저 수정했습니다.");
            }
        } else {
            salesment = new TB_Salesment();
        }

        LocalDateTime now = LocalDateTime.now();
        String statedt = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        // 2. 필드 매핑
        salesment.setIssuetype((String) form.get("IssueType")); // 발행형태
        salesment.setTaxtype((String) form.get("TaxType")); // 과세형태

        String invoiceeType = (String) form.get("InvoiceeType");
        salesment.setInvoiceetype(invoiceeType); // 거래처 유형

        salesment.setMisgubun(form.get("sale_type").toString()); // 매출구분
        salesment.setKwon(parseInt(form.get("Kwon"))); // 권
        salesment.setHo(parseInt(form.get("Ho"))); // 호
        salesment.setSerialnum((String) form.get("SerialNum")); // 일련번호

        // 공급자 사업장 정보 가져오기
        String spjangcd = (String) form.get("spjangcd");
        Map<String, Object> invoicer = this.getInvoicerDatail(spjangcd);
        if (invoicer != null) {
            String saupnum = (String) invoicer.get("saupnum");
            if (saupnum != null) {
                // 하이픈 제거
                saupnum = saupnum.replaceAll("-", "");
                salesment.setIcercorpnum(saupnum);
            }
            salesment.setIcercorpnm((String) invoicer.get("spjangnm"));
            salesment.setIcerceonm((String) invoicer.get("prenm"));
            salesment.setIceraddr((String) invoicer.get("address"));
            salesment.setIcerbiztype((String) invoicer.get("biztype"));
            salesment.setIcerbizclass((String) invoicer.get("item"));
            salesment.setIcerpernm((String) invoicer.get("agnertel1"));
            salesment.setIcertel((String) invoicer.get("agnertel2"));
            salesment.setIceremail((String) invoicer.get("emailadres"));
        }

        // 공급받는자
        salesment.setCltcd(parseInt(form.get("InvoiceeID")));
        String corpNum = sanitizeNumericString(form.get("InvoiceeCorpNum"));

        if ("개인".equals(invoiceeType)) {
            try {
                corpNum = EncryptionUtil.encrypt(corpNum); // 암호화된 값으로 교체
            } catch (Exception e) {
                throw new RuntimeException("암호화 실패", e);
            }
        }
        // 등록번호
        salesment.setIvercorpnum(corpNum);

        salesment.setIverregid((String) form.get("InvoiceeTaxRegID")); // 종사업장
        salesment.setIvercorpnm((String) form.get("InvoiceeCorpName")); // 사업장
        salesment.setIverceonm((String) form.get("InvoiceeCEOName")); // 대표자명
        salesment.setIveraddr((String) form.get("InvoiceeAddr")); // 주소
        salesment.setIverbiztype((String) form.get("InvoiceeBizType")); // 업태
        salesment.setIverbizclass((String) form.get("InvoiceeBizClass")); // 종목
        salesment.setIverpernm((String) form.get("InvoiceeContactName1")); // 담당자명
        salesment.setIvertel(sanitizeNumericString(form.get("InvoiceeTEL1"))); // 담당자 연락처
        salesment.setIveremail((String) form.get("InvoiceeEmail1")); // 이메일
        String misdate = sanitizeNumericString(form.get("writeDate"));
//        salesment = tb_salesmentRepository.save(salesment);


        salesment.setAccsubcode((String) form.get("account_codeHidden"));
        salesment.setDepartcode((String) form.get("att_departHidden"));
        salesment.setProjectcode((String) form.get("projectHidden"));
        salesment.setPurposetype((String) form.get("purpose_type"));

        salesment.setMgtkey("TAX-" + misdate + "-" + salesment.getMisnum());
        salesment.setMisdate(misdate);

        BigDecimal totalAmount = parseMoney(form.get("TotalAmount"));
        if (totalAmount != null) {
            salesment.setTotalamt(totalAmount.intValue()); // 합계금액
        }
        salesment.setSupplycost(parseIntSafe(form.get("SupplyCostTotal"))); // 총 공급가액
        salesment.setTaxtotal(parseIntSafe(form.get("TaxTotal"))); // 총 세액
        salesment.setRemark1((String) form.get("Remark1")); // 비고1

        Object remark2 = form.get("Remark2");
        if (remark2 != null && !remark2.toString().trim().isEmpty()) {
            salesment.setRemark2(remark2.toString().trim()); // 비고2
        }

        Object remark3 = form.get("Remark3");
        if (remark3 != null && !remark3.toString().trim().isEmpty()) {
            salesment.setRemark3(remark3.toString().trim()); // 비고3
        }

        salesment.setIssuediv((String) form.get("issue_div"));

        // 수정세금계산서일 때 저장
        salesment.setModifycd(parseInt(form.get("ModifyCode")));
        salesment.setOrgntscfnum(removeMinusSign(form.get("orgntscfnum")));
        salesment.setOrgmgtkey((String)form.get("orgmgtkey"));

        String issueDiv = (String) form.get("issue_div");
        // 팝빌 처리되는 코드값만 100
        if ("nextday".equals(issueDiv) || "issuenow".equals(issueDiv)) {
            salesment.setStatecode(100);
        } else {
            salesment.setStatecode(999);
        }
        salesment.setStatedt(statedt);

        if (isUpdate) {
            tb_salesDetailRepository.deleteByMisnum(misnum);   // 현재 PK로 삭제
        }

        salesment.setSpjangcd((String) form.get("spjangcd"));
        TB_Salesment saved = tb_salesmentRepository.save(salesment);

        // 3. 상세 목록 매핑
        int serialIndex = 1;
        List<TB_SalesDetail> details = new ArrayList<>();

        int i = 0;
        while (true) {
            String prefix = "detailList[" + i + "]";
            String itemName = (String) form.get(prefix + ".ItemName");

            if (itemName == null) break; // 더 이상 항목 없음

            if (itemName.trim().isEmpty()) {
                i++;
                continue;
            }

            String serialNum = String.valueOf(serialIndex++);

            TB_SalesDetail detail = new TB_SalesDetail();
            detail.setId(new TB_SalesDetailId(saved.getMisnum(), serialNum));
            detail.setMaterialId(parseInt(form.get(prefix + ".ItemId")));
            detail.setItemnm(itemName);
            detail.setMisdate(misdate);
            detail.setSpec((String) form.get(prefix + ".Spec"));
            BigDecimal qty = parseMoney(form.get(prefix + ".Qty"));
            if (qty != null) detail.setQty(qty.intValue());

            BigDecimal unitCost = parseMoney(form.get(prefix + ".UnitCost"));
            if (unitCost != null) detail.setUnitcost(unitCost.intValue());

            BigDecimal supplyCost = parseMoney(form.get(prefix + ".SupplyCost"));
            if (supplyCost != null) detail.setSupplycost(supplyCost.intValue());

            BigDecimal tax = parseMoney(form.get(prefix + ".Tax"));
            if (tax != null) detail.setTaxtotal(tax.intValue());

            detail.setRemark((String) form.get(prefix + ".Remark"));
            detail.setSpjangcd((String) form.get("spjangcd"));

            String purchaseDT = (String) form.get(prefix + ".PurchaseDT");
            if (purchaseDT != null && purchaseDT.length() == 4) {
                String fullPurchaseDT = misdate.substring(0, 4) + purchaseDT;
                detail.setPurchasedt(fullPurchaseDT);
            } else {
                detail.setPurchasedt(null);
            }

            detail.setSalesment(saved);
            details.add(detail);

            i++;
        }

        saved.getDetails().clear();
        saved.getDetails().addAll(details);

        return tb_salesmentRepository.save(saved);

    }

    private void updateShipmentLinks(Map<String, Object> form, TB_Salesment salesment) {
        // 5. shipment_head 업데이트
        Object shipIdsObj = form.get("shipids");

        if (shipIdsObj != null) {
            String shipIdsStr = shipIdsObj.toString(); // "165,162,164"
            List<Integer> shipIds = Arrays.stream(shipIdsStr.split(","))
                    .map(String::trim)
                    .filter(s -> !s.isEmpty())
                    .map(Integer::parseInt)
                    .toList();

            // shipment_head 엔티티들 조회 후 misnum 설정
            List<ShipmentHead> shipments = shipmentHeadRepository.findAllById(shipIds);
            for (ShipmentHead shipment : shipments) {
                shipment.setMisnum(salesment.getMisnum()); // misnum은 auto-generated 이거나 업데이트 대상
            }

            shipmentHeadRepository.saveAll(shipments);
        }
    }


    private Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }


    // 단건 사업자 검증
    public JsonNode validateSingleBusiness(String businessNumber) {
        try {
            String cleanBno = businessNumber.replaceAll("-", "");

            String url = "https://api.odcloud.kr/api/nts-businessman/v1/status?serviceKey=" + invoiceeCheckApiKey + "&returnType=JSON";
            URI uri = URI.create(url);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            String jsonBody = jacksonObjectMapper.writeValueAsString(Map.of("b_no", List.of(cleanBno)));

            HttpEntity<String> request = new HttpEntity<>(jsonBody, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(uri, request, String.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                JsonNode json = jacksonObjectMapper.readTree(response.getBody());
                JsonNode dataNode = json.path("data");
                if (dataNode.isArray() && dataNode.size() > 0) {
                    JsonNode item = dataNode.get(0);
                    String state = item.path("b_stt_cd").asText(); // "01": 정상
                    if (!"01".equals(state)) {
                        return null; // 휴업, 폐업 등 처리
                    }
                    return item;
                } else {
                    throw new RuntimeException("사업자 정보가 없습니다.");
                }
            } else {
                throw new RuntimeException("사업자 진위 확인 실패 - 응답 없음");
            }
        } catch (Exception e) {
            log.info("=== 사업자 진위확인 API 예외 발생 ===");
            log.info("에러 메시지     : {}", e.getMessage());
            e.printStackTrace();
            throw new RuntimeException("사업자 진위 확인 중 오류");
        }
    }

    // 다건 사업자 검증
    public List<JsonNode> validateMultipleBusinesses(List<String> businessNumbers) {
        try {
            List<String> cleanList = businessNumbers.stream()
                    .map(bno -> bno.replaceAll("-", ""))
                    .toList();

            String url = "https://api.odcloud.kr/api/nts-businessman/v1/validate?serviceKey=" +
                    URLEncoder.encode(invoiceeCheckApiKey, StandardCharsets.UTF_8);

            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);

            Map<String, Object> body = Map.of("b_no", cleanList);
            HttpEntity<Map<String, Object>> request = new HttpEntity<>(body, headers);

            ResponseEntity<String> response = restTemplate.postForEntity(url, request, String.class);

            if (response.getStatusCode() == HttpStatus.OK && response.getBody() != null) {
                JsonNode json = jacksonObjectMapper.readTree(response.getBody());
                JsonNode dataNode = json.path("data");
                if (dataNode.isArray()) {
                    List<JsonNode> results = new ArrayList<>();
                    dataNode.forEach(results::add);
                    return results;
                } else {
                    throw new RuntimeException("응답 데이터가 배열이 아닙니다.");
                }
            } else {
                throw new RuntimeException("사업자 진위 확인 실패 - 응답 없음");
            }
        } catch (Exception e) {
            throw new RuntimeException("사업자 진위 확인 중 오류");
        }
    }

    public List<Map<String, Object>> getShipmentHeadList(String dateFrom, String dateTo, Integer cltcd) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("dateFrom", dateFrom);
        paramMap.addValue("dateTo", dateTo);
        paramMap.addValue("cltcd", cltcd);

        String sql = """
                WITH material_summary AS (
                	SELECT\s
                		s."ShipmentHead_id",
                		STRING_AGG(s."Material_id"::text, ',' ORDER BY s."Material_id") AS material_ids
                	FROM shipment s
                	GROUP BY s."ShipmentHead_id"
                )
                			
                SELECT
                	sh.id,
                	sh."Company_id" AS company_id,
                	c."Name" AS company_name,
                	sh."ShipDate" AS ship_date,
                	sh."TotalQty" AS total_qty,
                	sh."TotalPrice" AS total_price,
                	sh."TotalVat" AS total_vat,
                	sh."TotalPrice" + sh."TotalVat" AS total_amount,
                	sh."Description" AS description,
                	sh."State" AS state,
                	fn_code_name('shipment_state', sh."State") AS state_name,
                	TO_CHAR(COALESCE(sh."OrderDate", sh."_created"), 'yyyy-mm-dd') AS order_date,
                	sh."StatementIssuedYN" AS issue_yn,
                	sh."StatementNumber" AS stmt_number,
                	sh."IssueDate" AS issue_date,
                	ms.material_ids
                FROM shipment_head sh
                JOIN company c
                	ON c.id = sh."Company_id"
                LEFT JOIN material_summary ms
                	ON ms."ShipmentHead_id" = sh.id
                WHERE sh."ShipDate" BETWEEN CAST(:dateFrom AS DATE) AND CAST(:dateTo AS DATE)
                  AND sh."State" = 'shipped'
                  AND sh."misnum" IS NULL
                """;

                 if (cltcd != null) {
                     sql += " AND sh.\"Company_id\" = :cltcd ";
                 }

                sql += """
                ORDER BY sh."ShipDate" DESC
                """;
        List<Map<String, Object>> items = this.sqlRunner.getRows(sql, paramMap);

        return items;
    }

    public Map<String, Object> getInvoiceDetail(Integer misnum) throws IOException {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("misnum", misnum);

        String sql = """ 
                SELECT\s
                	m.misdate,
                	m.vercode,
                	TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "writeDate",
                	m.misdate AS "mowriteDate",
                	m.misnum,
                	m.issuetype AS "IssueType",
                	m.taxtype AS "TaxType",
                	m.misgubun AS "sale_type",
                	m.kwon AS "Kwon",
                	m.ho AS "Ho",
                	m.serialnum AS "SerialNum",
                	m."invoiceetype" AS "InvoiceeType",
                	
                	m.icercorpnum AS "InvoicerCorpNum",
                	m.icerregid AS "InvoicerTaxRegID",
                	m.icercorpnm AS "InvoicerCorpName",
                	m.icerceonm AS "InvoicerCEOName",
                	m.iceraddr AS "InvoicerAddr",
                	m.icerbiztype AS "InvoicerBizType",
                	m.icerbizclass AS "InvoicerBizClass",
                	m.icerpernm AS "InvoicerContactName",
                	m.icertel AS "InvoicerTEL",
                	m.iceremail AS "InvoicerEmail",
                 
                	m.cltcd AS "InvoiceeID",
                	m.ivercorpnum AS "InvoiceeCorpNum",
                	m.iverregid AS "InvoiceeTaxRegID",
                	m.ivercorpnm AS "InvoiceeCorpName",
                	m.iverceonm AS "InvoiceeCEOName",
                	m.iveraddr AS "InvoiceeAddr",
                	m.iverbiztype AS "InvoiceeBizType",
                	m.iverbizclass AS "InvoiceeBizClass",
                	m.iverpernm AS "InvoiceeContactName1",
                	m.ivertel AS "InvoiceeTEL1",
                	m.iveremail AS "InvoiceeEmail1",
                 
                	m.supplycost AS "SupplyCostTotal",
                	m.taxtotal AS "TaxTotal",
                	m.remark1 AS "Remark1",
                	m.remark2 AS "Remark2",
                	m.remark3 AS "Remark3",
                	m.totalamt AS "TotalAmount",
                	m.cash AS "Cash",
                	m.chkbill AS "ChkBill",
                	m.note AS "Note",
                	m.credit AS "Credit",
                	m.purposetype AS "purpose_type",
                	m.statecode AS "StateCode",
                	sc1."Value" AS statecode_name,
                    m.accsubcode AS "account_codeHidden",
                    m.departcode AS "att_departHidden",
                    m.projectcode AS "projectHidden",
                 	sc3.accnm AS "account_code",
                 	sc4.projnm AS "project",
                 	sc5."Name" AS "att_depart",
                 	m.issuediv AS "issue_div",
                 	sc2."Description" AS ntscode_des,
                    CASE
                      WHEN m.ntscfnum IS NULL OR m.ntscfnum = '' OR LENGTH(m.ntscfnum) < 5 THEN NULL
                      ELSE SUBSTR(m.ntscfnum, 1, 8) || '-' ||
                           SUBSTR(m.ntscfnum, 9, 8) || '-' ||
                           SUBSTR(m.ntscfnum, 17)
                    END AS "ntscfnum",
                      
                    CASE
                      WHEN m.orgntscfnum IS NULL OR m.orgntscfnum = '' OR LENGTH(m.orgntscfnum) < 5 THEN NULL
                      ELSE SUBSTR(m.orgntscfnum, 1, 8) || '-' ||
                           SUBSTR(m.orgntscfnum, 9, 8) || '-' ||
                           SUBSTR(m.orgntscfnum, 17)
                    END AS "orgntscfnum",
                 	m.modifycd as "ModifyCode",
                  CASE m.modifycd
                      WHEN 1 THEN '기재사항 착오정정'
                      WHEN 2 THEN '공급가액 변동'
                      WHEN 3 THEN '환입'
                      WHEN 4 THEN '계약의 해제'
                      WHEN 5 THEN '내국신용장 사후개설'
                      WHEN 6 THEN '착오에 의한 이중발급'
                      ELSE NULL
                  END AS modify_name
                		 			
                FROM tb_salesment m
                		
                LEFT JOIN sys_code sc1
                	ON sc1."CodeType" = 'state_code_pb'
                	AND sc1."Code" = m.statecode::text
                  
                LEFT JOIN sys_code sc2
                	ON sc2."CodeType" = 'nts_code'
                	AND sc2."Code" = m.ntscode::text
                	
                LEFT JOIN tb_accsubject sc3
                    ON sc3."acccd" = m.accsubcode
                 
                 LEFT JOIN TB_DA003 sc4
                    ON sc4."projno" = m.projectcode
                 
                 LEFT JOIN depart sc5
                    ON sc5."Code" = m.departcode
                   
                WHERE m.misnum = :misnum
                """;

        String detailSql = """ 
                SELECT
                	 d."Material_id" AS "ItemId",
                	 d.itemnm AS "ItemName",
                	 d.spec AS "Spec",
                	 d.qty AS "Qty",
                	 d.unitcost AS "UnitCost",
                	 d.supplycost AS "SupplyCost",
                	 d.taxtotal AS "Tax",
                	 d.remark AS "Remark",
                	 SUBSTRING(d.purchasedt FROM 5 FOR 4) AS "PurchaseDT",
                	 d.misseq AS "SerialNum"
                 FROM tb_salesdetail d
                 WHERE d.misnum = :misnum
                 ORDER BY d.misseq::int asc
                """;

        Map<String, Object> master = this.sqlRunner.getRow(sql, paramMap);
        List<Map<String, Object>> detailList = this.sqlRunner.getRows(detailSql, paramMap);

        UtilClass.decryptItem(master, "InvoiceeCorpNum", 0);

        master.put("detailList", detailList);
        return master;
    }

    public Map<String, Object> getInvoicerDatail(String spjangcd) {

        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("spjangcd", spjangcd);

        String sql = """
                select "saupnum"
                , "spjangnm"
                , "adresa"
                , "adresb"
                , "prenm"
                , ("adresa" || ' ' || COALESCE("adresb", '')) AS address
                , "biztype"
                , "item"
                , "tel1"
                , "agnertel1"
                , "agnertel2"
                , "emailadres"
                from tb_xa012
                where spjangcd = :spjangcd
                """;

        Map<String, Object> item = this.sqlRunner.getRow(sql, paramMap);

        return item;
    }

    public AjaxResult issueInvoice(List<Map<String, String>> issueList) {
        AjaxResult result = new AjaxResult();

        if (issueList.isEmpty()) {
            result.success = false;
            result.message = "세금계산서가 선택되지 않았습니다.";
            return result;
        }

        List<Integer> idList = issueList.stream()
                .map(item -> Integer.parseInt(item.get("misnum")))
                .toList();

        List<TB_Salesment> salesList = tb_salesmentRepository.findAllByMisnumIn(idList);
        if (salesList.isEmpty()) {
            result.success = false;
            result.message = "해당 세금계산서를 찾을 수 없습니다.";
            return result;
        }

        // 발행 처리
        AjaxResult issueResult = callPopbillIssue(salesList);

        result.success = issueResult.success;
        result.message = issueResult.message;
        return result;
    }

    private AjaxResult callPopbillIssue(List<TB_Salesment> salesList) {
        AjaxResult result = new AjaxResult();
        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();

        for (TB_Salesment sales : salesList) {

            AjaxResult singleResult = callSingleIssue(sales); // 별도 트랜잭션
            if (singleResult.success) {
                successList.add("상호: " + sales.getIvercorpnm());
            } else {
                failList.add("상호: " + sales.getIvercorpnm() + " (" + singleResult.message + ")");
            }
        }

        if (failList.isEmpty()) {
            result.success = true;
            result.message = "총 " + salesList.size() + "건이 성공적으로 발행되었습니다.";
        } else {
            result.success = false;
            result.message = "일부 발행 실패: " + failList.size() + "건\n" + String.join("\n", failList);
        }

        return result;
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public AjaxResult callSingleIssue(TB_Salesment sm) {
        AjaxResult result = new AjaxResult();
        String statedt = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));

        try {
            Taxinvoice taxinvoice = makeTaxInvoiceObject(sm);

            try {
                ObjectMapper mapper = new ObjectMapper();
                log.info("=== 팝빌 요청 세금계산서 JSON ===");
                log.info(mapper.writeValueAsString(taxinvoice));
            } catch (JsonProcessingException e) {
                log.error("JSON 출력 실패: {}", e.getMessage());
            }

            String CorpNum = sm.getIcercorpnum();
            String mgtKey = sm.getMgtkey();

            // 1. 임시저장 실패 시 바로 catch 블록으로 빠지므로 state 변경 안됨
            taxinvoiceService.register(CorpNum, taxinvoice, null);

            // 2. 첨부파일
            String basePath = settings.getProperty("file_temp_upload_path");
            String filePath = basePath + "TAX-" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + "-" + sm.getMisnum() + ".pdf";
            File file = new File(filePath);
            log.info("PDF 파일 존재 여부: {}", file.exists());
            log.info("PDF 파일 크기: {} bytes", file.length());
            if (file.exists()) {
                try (FileInputStream fis = new FileInputStream(file)) {
                    taxinvoiceService.attachFile(
                            CorpNum,
                            MgtKeyType.SELL,
                            mgtKey,
                            "TAX-" + LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd")) + "-" + sm.getMisnum() + ".pdf",
                            fis
                    );
                    log.info("파일 첨부 성공: {}", filePath);
                } catch (Exception e) {
                    log.error("파일 첨부 실패: {}", e.getMessage());
                    sm.setStatecode(305);
                    sm.setStatedt(statedt);
                    tb_salesmentRepository.save(sm);
                    result.success = false;
                    result.message = "파일 첨부 실패: " + e.getMessage();
                    return result;
                }

                if (file.delete()) {
                    log.info("첨부 후 PDF 파일 삭제 성공: {}", filePath);
                } else {
                    log.info("첨부 후 PDF 파일 삭제 실패: {}", filePath);
                }
            }

            // 3. 발행
            IssueResponse response;
            try {
                response = taxinvoiceService.issue(
                        CorpNum,
                        MgtKeyType.SELL,
                        mgtKey,
                        "자동 발행 처리",
                        ""
                );
            } catch (Exception e) {
                log.error("발행 실패: {}", e.getMessage());
                sm.setStatecode(305);
                sm.setStatedt(statedt);
                tb_salesmentRepository.save(sm);
                result.success = false;
                result.message = "세금계산서 발행 실패: " + e.getMessage();
                return result;
            }

            // 즉시 발행일 경우 국세청 전송
            if ("issuenow".equalsIgnoreCase(sm.getIssuediv())) {
                try {
                    Response ntsResponse = taxinvoiceService.sendToNTS(
                            sm.getIcercorpnum(),
                            MgtKeyType.SELL,
                            sm.getMgtkey(),
                            ""
                    );
                    log.info("국세청 전송 요청 완료: {}", ntsResponse.getMessage());
                } catch (PopbillException ex) {
                    log.info("국세청 전송 요청 실패: {}", ex.getMessage());
                }
            }

            // 정상 처리 시
            sm.setStatecode(300);
            sm.setStatedt(statedt);
            sm.setNtscfnum(response.getNtsConfirmNum());
            tb_salesmentRepository.save(sm);

            result.success = true;
            result.message = "세금계산서가 발행되었습니다.";
        } catch (PopbillException e) {
            // register 실패 시만 여기에 옴 — 상태코드 변경 없음
            log.error("팝빌 오류 (임시저장): {}", e.getMessage());
            result.success = false;
            result.message = "팝빌 임시저장 실패: " + e.getMessage();
        }

        return result;
    }


    private Taxinvoice makeTaxInvoiceObject(TB_Salesment sm) {
        // LazyInitializationException 방지
        sm.getDetails().size();

        Taxinvoice invoice = new Taxinvoice();

        // 공급자 정보
        invoice.setWriteDate(sm.getMisdate()); // 작성일자 (yyyymmdd)
        invoice.setIssueType(sm.getIssuetype());       // 발행형태 - 정발행, 역발행 등
        invoice.setTaxType(sm.getTaxtype());           // 과세형태 - 과세, 영세, 면세
        invoice.setPurposeType(sm.getPurposetype());   // 영수/청구 구분
        invoice.setChargeDirection("정과금");

        invoice.setSupplyCostTotal(String.valueOf(Optional.ofNullable(sm.getSupplycost()).orElse(0)));
        invoice.setTaxTotal(String.valueOf(Optional.ofNullable(sm.getTaxtotal()).orElse(0)));
        invoice.setTotalAmount(String.valueOf(Optional.ofNullable(sm.getTotalamt()).orElse(0)));

        // 공급자 정보 설정
        invoice.setInvoicerCorpNum(sm.getIcercorpnum());
        invoice.setInvoicerCorpName(sm.getIcercorpnm());
        invoice.setInvoicerCEOName(sm.getIcerceonm());
        invoice.setInvoicerAddr(sm.getIceraddr());
        invoice.setInvoicerBizType(sm.getIcerbiztype());
        invoice.setInvoicerBizClass(sm.getIcerbizclass());
        invoice.setInvoicerContactName(sm.getIcerpernm());
        invoice.setInvoicerEmail(sm.getIceremail());
        invoice.setInvoicerTEL(sm.getIcertel());
//		invoice.setInvoicerMgtKey(sm.getId().getMisdate() + "-" + sm.getId().getMisnum());
        invoice.setInvoicerMgtKey(sm.getMgtkey());

        // 공급받는자 정보 설정
        invoice.setInvoiceeType(sm.getInvoiceetype());

        String invoicerCorpNum = sm.getIvercorpnum();
        if ("개인".equals(sm.getInvoiceetype())) {
            try {
                Map<String, Object> tempMap = new HashMap<>();
                tempMap.put("ivercorpnum", invoicerCorpNum);
                UtilClass.decryptItem(tempMap, "ivercorpnum", 0); // 마스킹 없이 복호화
                invoicerCorpNum = (String) tempMap.get("ivercorpnum");
            } catch (IOException e) {
                log.error("주민번호 복호화 실패: {}", e.getMessage());
            }
        }

        invoice.setInvoiceeCorpNum(invoicerCorpNum);
        invoice.setInvoiceeCorpName(sm.getIvercorpnm());
        invoice.setInvoiceeCEOName(sm.getIverceonm());
        invoice.setInvoiceeAddr(sm.getIveraddr());
        invoice.setInvoiceeBizType(sm.getIverbiztype());
        invoice.setInvoiceeBizClass(sm.getIverbizclass());
        invoice.setInvoiceeContactName1(sm.getIverpernm());
        invoice.setInvoiceeEmail1(sm.getIveremail());
        invoice.setInvoiceeTEL1(sm.getIvertel());
        invoice.setInvoiceeMgtKey(""); // 공급받는자 문서 번호 > 역발행일 경우 필수

        // 메모 및 기타 정보
        invoice.setRemark1(sm.getRemark1());
        invoice.setRemark2(sm.getRemark2());
        invoice.setRemark3(sm.getRemark3());

        // 수정 세금계산서 정보
        Optional.ofNullable(sm.getModifycd())
                .map(Integer::shortValue)
                .ifPresent(invoice::setModifyCode);
        invoice.setOrgNTSConfirmNum(sm.getOrgntscfnum());
        invoice.setOriginalTaxinvoiceKey(sm.getOrgmgtkey());

        // 세부 품목 정보 설정
        List<TaxinvoiceDetail> detailList = sm.getDetails().stream().map(d -> {
            TaxinvoiceDetail detail = new TaxinvoiceDetail();
            detail.setSerialNum(Short.parseShort(d.getId().getMisseq()));
            detail.setItemName(Optional.ofNullable(d.getItemnm()).orElse(""));
            detail.setSpec(Optional.ofNullable(d.getSpec()).orElse(""));
            if (d.getQty() != null) {
                detail.setQty(String.valueOf(d.getQty()));
            }

            if (d.getUnitcost() != null) {
                detail.setUnitCost(String.valueOf(d.getUnitcost()));
            }

            if (d.getSupplycost() != null) {
                detail.setSupplyCost(String.valueOf(d.getSupplycost()));
            }

            if (d.getTaxtotal() != null) {
                detail.setTax(String.valueOf(d.getTaxtotal()));
            }

            detail.setRemark(Optional.ofNullable(d.getRemark()).orElse(""));
            return detail;
        }).toList();

        invoice.setDetailList(detailList);


        return invoice;
    }

    @Transactional
    public AjaxResult deleteSalesment(List<Map<String, String>> deleteList) {
        AjaxResult result = new AjaxResult();

        if (deleteList == null || deleteList.isEmpty()) {
            result.success = false;
            result.message = "삭제할 데이터가 없습니다.";
            return result;
        }

        List<Integer> idList = deleteList.stream()
                .map(item -> Integer.parseInt(item.get("misnum")))
                .toList();

        // 1. 관련된 shipment_head의 misnum 컬럼을 null로 변경
        List<ShipmentHead> relatedShipments = shipmentHeadRepository.findByMisnumIn(idList);
        for (ShipmentHead shipment : relatedShipments) {
            shipment.setMisnum(null);
        }
        shipmentHeadRepository.saveAll(relatedShipments);

        // 2. salesdetail 삭제
        deleteBySalesdetailIds(idList);

        // 3. salesment 삭제
        tb_salesmentRepository.deleteAllById(idList);

        result.success = true;
        return result;
    }

    public void deleteBySalesdetailIds(List<Integer> idList) {
        if (idList == null || idList.isEmpty()) return;

        String placeholders = idList.stream()
                .map(id -> "?")
                .collect(Collectors.joining(", "));

        String sql = "DELETE FROM tb_salesdetail WHERE misnum IN (" + placeholders + ")";
        jdbcTemplate.update(sql, idList.toArray());
    }

    private Integer parseInt(Object value) {
        if (value == null) return null;
        try {
            return Integer.parseInt(value.toString().trim());
        } catch (Exception e) {
            return null; // 또는 0
        }
    }

    private BigDecimal parseMoney(Object obj) {
        if (obj == null || obj.toString().trim().isEmpty()) return null;
        try {
            return new BigDecimal(obj.toString().replaceAll(",", "").trim());
        } catch (Exception e) {
            return null;
        }
    }

    private String sanitizeNumericString(Object obj) {
        if (obj == null) return null;
        return obj.toString().replaceAll("[^0-9]", "");  // 숫자만 남김
    }

    private Integer parseIntSafe(Object obj) {
        if (obj == null) return null;
        String str = obj.toString().replaceAll(",", "").trim();
        if (!str.matches("-?\\d+")) return null; // 정수 패턴 검사: optional 음수부호 + 숫자
        return Integer.parseInt(str);
    }

    private String removeMinusSign(Object obj) {
        if (obj == null) return null;
        return obj.toString().replaceAll("-", ""); // '-'만 정규식으로 제거
    }

    @Transactional
    public AjaxResult cancelIssue(List<Map<String, String>> cancelList) {
        AjaxResult result = new AjaxResult();

        if (cancelList == null || cancelList.isEmpty()) {
            result.success = false;
            result.message = "취소할 데이터가 없습니다.";
            return result;
        }

        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();

        for (Map<String, String> item : cancelList) {
            try {
                Integer misnum = Integer.parseInt(item.get("misnum"));
                TB_Salesment sm = tb_salesmentRepository.findById(misnum).orElse(null);
                if (sm == null) {
                    failList.add("misnum: " + misnum + " (해당 내역 없음)");
                    continue;
                }

                String corpNum = sm.getIcercorpnum();
                String mgtKey = sm.getMgtkey();

                Response response = taxinvoiceService.cancelIssue(
                        corpNum,
                        MgtKeyType.SELL,
                        mgtKey,
                        "",
                        ""
                );

                log.info("팝빌 발행 취소 결과 === misnum: {}", misnum);
                log.info("code: {}", response.getCode());
                log.info("message: {}", response.getMessage());

                if (response.getCode() == 1) {
                    sm.setStatecode(600); // 발행 취소
                    LocalDateTime now = LocalDateTime.now();
                    String statedt = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
                    sm.setStatedt(statedt);
                    tb_salesmentRepository.save(sm);
                    successList.add("상호: " + sm.getIvercorpnm());
                } else {
                    failList.add("상호: " + sm.getIvercorpnm() + " (" + response.getMessage() + ")");
                }

            } catch (Exception e) {
                failList.add("처리 중 오류 발생 (" + e.getMessage() + ")");
                e.printStackTrace();
            }
        }

        if (failList.isEmpty()) {
            result.success = true;
            result.message = "총 " + successList.size() + "건의 발행이 취소되었습니다.";
        } else {
            result.success = false;
            result.message = "일부 발행 취소 실패: " + failList.size() + "건\n" + String.join("\n", failList);
        }

        return result;
    }

    @Transactional
    public AjaxResult updateinvoice(Integer misnum, String issuediv) {
        AjaxResult result = new AjaxResult();

        TB_Salesment sm = tb_salesmentRepository.findById(misnum).orElse(null);
        if (sm == null) {
            result.success = false;
            result.message = "해당 데이터가 존재하지 않습니다.";
            return result;
        }

        sm.setIssuediv(issuediv);

        if ("other".equals(issuediv) || "reverse".equals(issuediv)) {
            sm.setStatecode(999);
        } else {
            sm.setStatecode(100);
        }

        tb_salesmentRepository.save(sm);

        return result;
    }

    @Transactional
    public AjaxResult reMessage(List<Map<String, String>> invoiceList) {
        AjaxResult result = new AjaxResult();

        if (invoiceList == null || invoiceList.isEmpty()) {
            result.success = false;
            result.message = "재전송 할 데이터가 없습니다.";
            return result;
        }

        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();

        for (Map<String, String> item : invoiceList) {
            try {
                Integer misnum = Integer.parseInt(item.get("misnum"));
                TB_Salesment sm = tb_salesmentRepository.findById(misnum).orElse(null);
                if (sm == null) {
                    failList.add("misnum: " + misnum + " (해당 내역 없음)");
                    continue;
                }

                String corpNum = sm.getIcercorpnum();
                String mgtKey = sm.getMgtkey();
                String[] emailKeys = {"email1", "email2", "email3"};

                for (String key : emailKeys) {
                    String email = item.get(key);
                    if (email != null && !email.trim().isEmpty()) {
                        Response response = taxinvoiceService.sendEmail(
                                corpNum,
                                MgtKeyType.SELL,
                                mgtKey,
                                email.trim(),
                                ""
                        );

                        log.info("재전송 결과 === misnum: {}", misnum + ", email: " + email);
                        log.info("code: {}", response.getCode());
                        log.info("message: {}", response.getMessage());

                        if (response.getCode() == 1) {
                            successList.add("상호: " + sm.getIvercorpnm() + " (이메일: " + email + ")");
                        } else {
                            failList.add("상호: " + sm.getIvercorpnm() + " (이메일: " + email + ", 오류: " + response.getMessage() + ")");
                        }
                    }
                }

            } catch (Exception e) {
                failList.add("처리 중 오류 발생 (" + e.getMessage() + ")");
                e.printStackTrace();
            }
        }

        if (failList.isEmpty()) {
            result.success = true;
            result.message = "총 " + successList.size() + "건이 재전송되었습니다.";
        } else {
            result.success = false;
            result.message = "일부 재전송 실패: " + failList.size() + "건\n" + String.join("\n", failList);
        }

        return result;
    }

    @Transactional
    public AjaxResult deleteInvoice(List<Map<String, String>> delList) {
        AjaxResult result = new AjaxResult();

        if (delList == null || delList.isEmpty()) {
            result.success = false;
            result.message = "삭제 할 데이터가 없습니다.";
            return result;
        }

        List<String> successList = new ArrayList<>();
        List<String> failList = new ArrayList<>();
        List<Integer> successMisnums = new ArrayList<>();

        for (Map<String, String> item : delList) {
            try {
                Integer misnum = Integer.parseInt(item.get("misnum"));
                TB_Salesment sm = tb_salesmentRepository.findById(misnum).orElse(null);
                if (sm == null) {
                    failList.add("misnum: " + misnum + " (해당 내역 없음)");
                    continue;
                }

                String corpNum = sm.getIcercorpnum();
                String mgtKey = sm.getMgtkey();

                Response response = taxinvoiceService.delete(
                        corpNum,
                        MgtKeyType.SELL,
                        mgtKey,
                        ""
                );

                if (response.getCode() == 1) {
                    successList.add("상호: " + sm.getIvercorpnm());
                    successMisnums.add(misnum);
                } else {
                    failList.add("상호: " + sm.getIvercorpnm() + " (" + response.getMessage() + ")");
                }

            } catch (Exception e) {
                failList.add("처리 중 오류 발생 (" + e.getMessage() + ")");
                e.printStackTrace();
            }
        }

        result.success = failList.isEmpty();
        result.message = result.success
                ? "총 " + successList.size() + "건이 삭제되었습니다."
                : "일부 삭제 실패: " + failList.size() + "건\n" + String.join("\n", failList);

        result.data = Map.of("successMisnums", successMisnums);

        return result;
    }

    @Transactional
    public AjaxResult copyInvoice(List<Map<String, String>> copyList) {
        AjaxResult result = new AjaxResult();

        if (copyList == null || copyList.isEmpty()) {
            result.success = false;
            result.message = "복사할 데이터가 없습니다.";
            return result;
        }


        for (Map<String, String> item : copyList) {
            try {
                Integer misnum = Integer.parseInt(item.get("misnum"));
                String misdate = sanitizeNumericString(item.get("writedate"));
                TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();
                TB_Salesment copy = new TB_Salesment();

                BeanUtils.copyProperties(origin, copy,
                        "misnum", "writedate", "misdate",
                        "mgtkey", "orgntscfnum", "orgmgtkey",
                        "modifycd", "statedt", "ntscode", "statecode",
                        "details", "ntscfnum"
                );

                copy.setMisnum(null); // 새 엔티티
                copy.setMisdate(misdate); // 새 날짜만 지정

                // 조건부 상태코드 설정
                copy.setStatecode(origin.getStatecode() == 999 ? 999 : 100);

                TB_Salesment savedCopy = tb_salesmentRepository.save(copy);

                savedCopy = tb_salesmentRepository.save(savedCopy);

                savedCopy.setMgtkey("TAX-" + misdate + "-" + savedCopy.getMisnum());
                savedCopy = tb_salesmentRepository.save(savedCopy);

                List<TB_SalesDetail> details = tb_salesDetailRepository.findByMisnum(misnum);
                int sequence = 1;

                for (TB_SalesDetail detail : details) {
                    TB_SalesDetail newDetail = new TB_SalesDetail();
                    TB_SalesDetailId newId = new TB_SalesDetailId(savedCopy.getMisnum(), String.valueOf(sequence++));
                    newDetail.setId(newId);

                    newDetail.setMisdate(misdate);
                    newDetail.setItemnm(detail.getItemnm());
                    newDetail.setSpec(detail.getSpec());
                    newDetail.setQty(detail.getQty());
                    newDetail.setUnitcost(detail.getUnitcost());
                    newDetail.setSupplycost(detail.getSupplycost());
                    newDetail.setTaxtotal(detail.getTaxtotal());
                    newDetail.setTotalamt(detail.getTotalamt());
                    newDetail.setRemark(detail.getRemark());
                    newDetail.setPurchasedt(misdate);
                    newDetail.setMaterialId(detail.getMaterialId());
                    newDetail.setSpjangcd(detail.getSpjangcd());

                    tb_salesDetailRepository.save(newDetail);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return result;
    }

    @Transactional
    public AjaxResult saveModifiedInvoice(@RequestBody Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        String modifyCode = (String) form.get("ModifyCode");

        if (modifyCode == null || modifyCode.isBlank()) {
            result.success = false;
            result.message = "수정사유 코드가 누락되었습니다.";
            return result;
        }

        switch (modifyCode) {
            case "1": // 기재사항 착오정정
                result = handleCorrection(form);
                break;
            case "2": // 공급가액 변동
                result = handlePriceChange(form);
                break;
            case "3": // 환입
                result = handleReturn(form);
                break;
            case "4": // 계약의 해제
                result = handleContractCancellation(form);
                break;
            case "5": // 내국신용장 사후개설
                result = handleLaterLetterOfCredit(form);
                break;
            case "6": // 착오에 의한 이중발급
                result = handleDuplicateIssue(form);
                break;
            default:
                result.success = false;
                result.message = "알 수 없는 수정사유 코드입니다.";
                break;
        }

        return result;

    }

    // 기재사항 착오정정
    private AjaxResult handleCorrection(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));

        try {
            // 여기 부터는 취소분 저장
            TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();
            TB_Salesment copy = new TB_Salesment();

            BeanUtils.copyProperties(origin, copy,
                    "misnum", "statedt", "ntscode", "statecode",
                    "details", "ntscfnum", "mgtkey"
            );

            copy.setMisnum(null); // 새 엔티티
            copy.setOrgntscfnum(origin.getNtscfnum());
            copy.setOrgmgtkey(origin.getMgtkey());
            copy.setModifycd(parseInt(form.get("ModifyCode")));
            copy.setSupplycost(Optional.ofNullable(copy.getSupplycost()).orElse(0) * -1);
            copy.setTaxtotal(Optional.ofNullable(copy.getTaxtotal()).orElse(0) * -1);
            copy.setTotalamt(Optional.ofNullable(copy.getTotalamt()).orElse(0) * -1);
            LocalDateTime now = LocalDateTime.now();
            String statedt = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            copy.setStatedt(statedt);

            // 조건부 상태코드 설정
            copy.setStatecode(100);

            TB_Salesment savedCopy = tb_salesmentRepository.save(copy);

            savedCopy.setMgtkey("TAX-" + savedCopy.getMisdate() + "-" + savedCopy.getMisnum());
            tb_salesmentRepository.save(savedCopy);

            List<TB_SalesDetail> details = tb_salesDetailRepository.findByMisnum(misnum);
            int sequence = 1;

            for (TB_SalesDetail detail : details) {
                TB_SalesDetail newDetail = new TB_SalesDetail();
                TB_SalesDetailId newId = new TB_SalesDetailId(savedCopy.getMisnum(), String.valueOf(sequence++));
                newDetail.setId(newId);

                newDetail.setMisdate(detail.getMisdate());
                newDetail.setItemnm(detail.getItemnm());
                newDetail.setSpec(detail.getSpec());
                newDetail.setQty(detail.getQty());
                if (detail.getUnitcost() != null) {
                    newDetail.setUnitcost(detail.getUnitcost() * -1);
                }
                if (detail.getSupplycost() != null) {
                    newDetail.setSupplycost(detail.getSupplycost() * -1);
                }
                if (detail.getTaxtotal() != null) {
                    newDetail.setTaxtotal(detail.getTaxtotal() * -1);
                }
                if (detail.getTotalamt() != null) {
                    newDetail.setTotalamt(detail.getTotalamt() * -1);
                }
                newDetail.setRemark(detail.getRemark());
                newDetail.setPurchasedt(detail.getPurchasedt());
                newDetail.setMaterialId(detail.getMaterialId());
                newDetail.setSpjangcd(detail.getSpjangcd());

                tb_salesDetailRepository.save(newDetail);
            }

            // 여기 부터는 수정분 저장
            form.put("orgntscfnum", origin.getNtscfnum());
            form.put("orgmgtkey", origin.getMgtkey());
            form.remove("misnum");
            form.remove("shipids");
            saveSalesInvoiceInternal(form);

        } catch (Exception e) {
            log.error("handleCorrection 오류", e);
            throw new RuntimeException("정정 처리 중 예외 발생", e);
        }

        result.success = true;
        result.message = "기재사항 착오정정 처리 완료";
        return result;
    }

    private AjaxResult handlePriceChange(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));
        TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();

        form.put("orgntscfnum", origin.getNtscfnum());
        form.put("orgmgtkey", origin.getMgtkey());
        form.remove("misnum");
        form.remove("shipids");
        saveSalesInvoiceInternal(form);

        result.success = true;
        result.message = "공급가액 변동 처리 완료";
        return result;
    }

    private AjaxResult handleReturn(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));
        TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();

        form.put("orgntscfnum", origin.getNtscfnum());
        form.put("orgmgtkey", origin.getMgtkey());
        form.remove("misnum");
        form.remove("shipids");
        saveSalesInvoiceInternal(form);

        result.success = true;
        result.message = "환입 처리 완료";
        return result;
    }

    private AjaxResult handleContractCancellation(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));
        TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();

        form.put("orgntscfnum", origin.getNtscfnum());
        form.put("orgmgtkey", origin.getMgtkey());
        form.remove("misnum");
        form.remove("shipids");
        saveSalesInvoiceInternal(form);

        result.success = true;
        result.message = "계약의 해제 처리 완료";
        return result;
    }

    private AjaxResult handleLaterLetterOfCredit(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));

        try {
            // 여기 부터는 취소분 저장
            TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();
            TB_Salesment copy = new TB_Salesment();

            BeanUtils.copyProperties(origin, copy,
                    "misnum", "statedt", "ntscode", "statecode",
                    "details", "ntscfnum", "mgtkey"
            );

            copy.setMisnum(null); // 새 엔티티
            copy.setOrgntscfnum(origin.getNtscfnum());
            copy.setOrgmgtkey(origin.getMgtkey());
            copy.setModifycd(parseInt(form.get("ModifyCode")));

            String taxType = copy.getTaxtype(); // 과세, 영세, 면세 등

            BigDecimal supplyCost = parseMoney(form.get("SupplyCostTotal"));
            BigDecimal taxTotal;
            BigDecimal totalAmt;

            if ("과세".equals(taxType)) {
                taxTotal = supplyCost.multiply(BigDecimal.valueOf(0.1)).setScale(0, RoundingMode.HALF_UP);
                totalAmt = supplyCost.add(taxTotal);
            } else {
                taxTotal = BigDecimal.ZERO;
                totalAmt = supplyCost;
            }

            copy.setSupplycost(supplyCost.intValue() * -1);
            copy.setTaxtotal(taxTotal.intValue() * -1);
            copy.setTotalamt(totalAmt.intValue() * -1);

            LocalDateTime now = LocalDateTime.now();
            String statedt = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            copy.setStatedt(statedt);

            // 조건부 상태코드 설정
            copy.setStatecode(100);

            TB_Salesment savedCopy = tb_salesmentRepository.save(copy);

            savedCopy.setMgtkey("TAX-" + savedCopy.getMisdate() + "-" + savedCopy.getMisnum());
            tb_salesmentRepository.save(savedCopy);

            int serialIndex = 1;
            List<TB_SalesDetail> details = new ArrayList<>();

            int i = 0;
            while (true) {
                String prefix = "detailList[" + i + "]";
                String itemName = (String) form.get(prefix + ".ItemName");

                if (itemName == null) break; // 더 이상 항목 없음
                if (itemName.trim().isEmpty()) {
                    i++;
                    continue;
                }

                String serialNum = String.valueOf(serialIndex++);

                TB_SalesDetail detail = new TB_SalesDetail();
                detail.setId(new TB_SalesDetailId(savedCopy.getMisnum(), serialNum));
                detail.setMaterialId(parseInt(form.get(prefix + ".ItemId")));
                detail.setItemnm(itemName);
                detail.setMisdate(savedCopy.getMisdate());
                detail.setSpec((String) form.get(prefix + ".Spec"));
                BigDecimal qty = parseMoney(form.get(prefix + ".Qty"));
                if (qty != null) detail.setQty(qty.intValue());

                BigDecimal unitCost = parseMoney(form.get(prefix + ".UnitCost"));
                if (unitCost != null) detail.setUnitcost(unitCost.intValue() * -1);

                BigDecimal supplyCostValue = parseMoney(form.get(prefix + ".SupplyCost"));
                if (supplyCostValue != null) detail.setSupplycost(supplyCostValue.intValue() * -1);

                BigDecimal tax = parseMoney(form.get(prefix + ".Tax"));
                if (tax != null) detail.setTaxtotal(tax.intValue() * -1);
                detail.setRemark((String) form.get(prefix + ".Remark"));
                detail.setSpjangcd((String) form.get("spjangcd"));

                String purchaseDT = (String) form.get(prefix + ".PurchaseDT");
                if (purchaseDT != null && purchaseDT.length() == 4) {
                    String fullPurchaseDT = savedCopy.getMisdate().substring(0, 4) + purchaseDT;
                    detail.setPurchasedt(fullPurchaseDT);
                } else {
                    detail.setPurchasedt(null);
                }

                detail.setSalesment(savedCopy);
                tb_salesDetailRepository.save(detail);

                i++;
            }


            // 여기 부터는 수정분 저장
            form.put("orgntscfnum", origin.getNtscfnum());
            form.put("orgmgtkey", origin.getMgtkey());
            form.remove("misnum");
            form.remove("shipids");
            saveSalesInvoiceInternal(form);

        } catch (Exception e) {
            log.error("handleCorrection 오류", e);
            throw new RuntimeException("정정 처리 중 예외 발생", e);
        }

        result.success = true;
        result.message = "내국신용장 사후개설 처리 완료";
        return result;
    }

    
    // 이중발급
    private AjaxResult handleDuplicateIssue(Map<String, Object> form) {
        AjaxResult result = new AjaxResult();

        Integer misnum = parseInt(form.get("misnum"));
        long start = System.currentTimeMillis();

        try {
            // 취소분 저장
            TB_Salesment origin = tb_salesmentRepository.findById(misnum).orElseThrow();
            TB_Salesment copy = new TB_Salesment();

            BeanUtils.copyProperties(origin, copy,
                    "misnum", "statedt", "ntscode", "statecode",
                    "details", "ntscfnum", "mgtkey"
            );
            log.info("복사 시간: {}ms", System.currentTimeMillis() - start);

            copy.setMisnum(null); // 새 엔티티
            copy.setOrgntscfnum(origin.getNtscfnum());
            copy.setOrgmgtkey(origin.getMgtkey());
            copy.setModifycd(parseInt(form.get("ModifyCode")));
            copy.setSupplycost(Optional.ofNullable(copy.getSupplycost()).orElse(0) * -1);
            copy.setTaxtotal(Optional.ofNullable(copy.getTaxtotal()).orElse(0) * -1);
            copy.setTotalamt(Optional.ofNullable(copy.getTotalamt()).orElse(0) * -1);
            LocalDateTime now = LocalDateTime.now();
            String statedt = now.format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"));
            copy.setStatedt(statedt);

            // 조건부 상태코드 설정
            copy.setStatecode(100);

            TB_Salesment savedCopy = tb_salesmentRepository.save(copy);

            savedCopy.setMgtkey("TAX-" + savedCopy.getMisdate() + "-" + savedCopy.getMisnum());
            tb_salesmentRepository.save(savedCopy);

            List<TB_SalesDetail> details = tb_salesDetailRepository.findByMisnum(misnum);
            int sequence = 1;

            for (TB_SalesDetail detail : details) {
                TB_SalesDetail newDetail = new TB_SalesDetail();
                TB_SalesDetailId newId = new TB_SalesDetailId(savedCopy.getMisnum(), String.valueOf(sequence++));
                newDetail.setId(newId);

                newDetail.setMisdate(detail.getMisdate());
                newDetail.setItemnm(detail.getItemnm());
                newDetail.setSpec(detail.getSpec());
                newDetail.setQty(detail.getQty());
                if (detail.getUnitcost() != null) {
                    newDetail.setUnitcost(detail.getUnitcost() * -1);
                }
                if (detail.getSupplycost() != null) {
                    newDetail.setSupplycost(detail.getSupplycost() * -1);
                }
                if (detail.getTaxtotal() != null) {
                    newDetail.setTaxtotal(detail.getTaxtotal() * -1);
                }
                if (detail.getTotalamt() != null) {
                    newDetail.setTotalamt(detail.getTotalamt() * -1);
                }
                newDetail.setRemark(detail.getRemark());
                newDetail.setPurchasedt(detail.getPurchasedt());
                newDetail.setMaterialId(detail.getMaterialId());
                newDetail.setSpjangcd(detail.getSpjangcd());

                tb_salesDetailRepository.save(newDetail);
            }

        } catch (Exception e) {
            log.error("handleCorrection 오류", e);
            throw new RuntimeException("정정 처리 중 예외 발생", e);
        }

        result.success = true;
        result.message = "착오에 의한 이중발급 처리 완료";
        return result;
    }


    @Transactional
    public AjaxResult saveInvoiceBulkData(MultipartFile upload_file, String spjangcd, User user) throws IOException {
        AjaxResult result = new AjaxResult();
        List<Map<String, Object>> errorList = new ArrayList<>();

        DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
        String formattedDate = dtf.format(LocalDateTime.now());
        String upload_filename = settings.getProperty("file_temp_upload_path") + formattedDate + "_" + upload_file.getOriginalFilename();

        File file = new File(upload_filename);
        if (file.exists()) file.delete();
        try (FileOutputStream fos = new FileOutputStream(file)) {
            fos.write(upload_file.getBytes());
        }

        FileInputStream fis = new FileInputStream(file);
        XSSFWorkbook wb = new XSSFWorkbook(fis);
        XSSFSheet sheet = wb.getSheetAt(0);

        for (int r = 6; r <= sheet.getLastRowNum(); r++) {
            XSSFRow row = sheet.getRow(r); // 7번째 행 (index 6)

            if (isRowEmpty(row)) continue;

            String ivercorpnum = getString(row.getCell(10));
            String misdate = getString(row.getCell(1));
            String year = misdate.substring(0, 4);

            // 사업자 번호일때만 휴폐업 조회
            if (ivercorpnum.length() != 13 && validateSingleBusiness(ivercorpnum) == null) {
                Map<String, Object> err = new HashMap<>();
                err.put("row", r + 2);
                err.put("message", "휴/폐업 사업자번호입니다. 공급받는자 등록번호를 확인해주세요.");
                errorList.add(err);
                continue;
            }

            try {
                TB_Salesment sm = new TB_Salesment();
                sm.setTaxtype(getString(row.getCell(0))); // 과세, 영세
                sm.setMisdate(misdate); // 작성일자
                sm.setIssuetype("정발행");
                sm.setIcercorpnum(getString(row.getCell(2))); // 공급자 등록번호
                sm.setIcerregid(getString(row.getCell(3))); // 공급자 종사업자 번호
                sm.setIcercorpnm(getString(row.getCell(4))); // 공급자 상호
                sm.setIcerceonm(getString(row.getCell(5))); // 공급자 대표명
                sm.setIceraddr(getString(row.getCell(6))); // 공급자 주소
                sm.setIcerbiztype(getString(row.getCell(7))); // 공급자 업태
                sm.setIcerbizclass(getString(row.getCell(8))); // 공급자 종목
                sm.setIceremail(getString(row.getCell(9))); // 공급자 이메일
                sm.setIvercorpnum(getString(row.getCell(10))); // 공급받는자 등록번호
                sm.setIverregid(getString(row.getCell(11))); // 공급받는자 종사업장 번호
                sm.setIvercorpnm(getString(row.getCell(12))); // 공급받는자 상호
                sm.setIverceonm(getString(row.getCell(13)));  // 공급받는자 대표명
                sm.setIveraddr(getString(row.getCell(14))); // 공급받는자 주소
                sm.setIverbiztype(getString(row.getCell(15))); // 공급받는자 업태
                sm.setIverbizclass(getString(row.getCell(16))); // 공급받는자 종목
                sm.setIveremail(getString(row.getCell(17))); // 공급받는자 이메일
                sm.setSupplycost(parseInt(row.getCell(18))); // 공급가액
                sm.setTaxtotal(parseInt(row.getCell(19))); // 세액
                sm.setTotalamt(sm.getSupplycost() + sm.getTaxtotal()); // 합계
                sm.setRemark1(getString(row.getCell(20))); // 비고
                sm.setCash(parseInt(row.getCell(21))); // 현금
                sm.setChkbill(parseInt(row.getCell(22))); // 수표
                sm.setCredit(parseInt(row.getCell(23))); // 어음
                sm.setNote(parseInt(row.getCell(24))); // 외상미수금
                sm.setPurposetype(getString(row.getCell(25))); // 영수/청구
                sm.setIssuediv("other");
                sm.setSpjangcd(spjangcd);

                if (ivercorpnum.length() == 10){
                    sm.setInvoiceetype("사업자");
                } else {
                    if ("9999999999999".equals(ivercorpnum)){
                        sm.setInvoiceetype("외국인");
                    }
                    sm.setInvoiceetype("개인");
                }


                // 사업자 번호 검색후 id 넣어주기
                Optional<Company> com = companyRepository.findByBusinessNumber(ivercorpnum);
                if (com.isPresent()) {
                    sm.setCltcd(com.get().getId());
                } else{
                    Map<String, Object> paramMap = new HashMap<>();
                    paramMap.put("InvoiceeCorpNum", ivercorpnum);
                    paramMap.put("InvoiceeAddr", sm.getIveraddr());
                    paramMap.put("InvoiceeBizClass", sm.getIverbizclass());
                    paramMap.put("InvoiceeBizType", sm.getIverbiztype());
                    paramMap.put("InvoiceeCEOName", sm.getIverceonm());
                    paramMap.put("InvoiceeContactName1", ""); // 필요시 지정
                    paramMap.put("InvoiceeEmail1", sm.getIveremail());
                    paramMap.put("InvoiceeTEL1", ""); // 필요시 지정
                    paramMap.put("InvoiceeCorpName", sm.getIvercorpnm());
                    paramMap.put("spjangcd", spjangcd);

                    AjaxResult compResult = saveInvoicee(paramMap, user);
                    if (!compResult.success) {
                        Map<String, Object> err = new HashMap<>();
                        err.put("row", r + 1);
                        err.put("message", compResult.message);
                        errorList.add(err);
                        continue;
                    }

                    Company newComp = (Company) compResult.data;
                    sm.setCltcd(newComp.getId());
                }

                // 주민번호 암호화
                if (ivercorpnum.length() == 13) {
                    try {
                        sm.setIvercorpnum(EncryptionUtil.encrypt(ivercorpnum));
                    } catch (Exception e) {
                        throw new RuntimeException("암호화 실패", e);
                    }

                } else {
                    sm.setIvercorpnum(ivercorpnum);
                }

                tb_salesmentRepository.save(sm);

                // 품목 처리 (AA열부터 = index 26, 8열 단위)
                int itemStartCol = 26;
                int itemBlockSize = 8;
                int seq = 1;

                for (int i = itemStartCol; i + 7 < row.getLastCellNum(); i += itemBlockSize) {
                    String itemnm = getString(row.getCell(i + 1));
                    if (itemnm == null || itemnm.isBlank()) break;

                    TB_SalesDetail detail = new TB_SalesDetail();
                    TB_SalesDetailId detailId = new TB_SalesDetailId();
                    detailId.setMisnum(sm.getMisnum());
                    detailId.setMisseq(String.valueOf(seq++));

                    detail.setId(detailId);
                    detail.setMisdate(misdate);
                    detail.setSalesment(sm);
                    detail.setItemnm(itemnm);
                    String datePart = getString(row.getCell(i));
                    detail.setPurchasedt(year + datePart);
                    detail.setQty(parseInt(row.getCell(i + 3)));
                    detail.setUnitcost(parseInt(row.getCell(i + 4)));
                    detail.setSupplycost(parseInt(row.getCell(i + 5)));
                    detail.setTaxtotal(parseInt(row.getCell(i + 6)));
                    detail.setTotalamt(detail.getSupplycost() + detail.getTaxtotal());
                    detail.setRemark(getString(row.getCell(i + 7)));

                    tb_salesDetailRepository.save(detail);
                }
                result.success = true;
            } catch (Exception e) {
                Map<String, Object> err = new HashMap<>();
                err.put("message", e.getMessage());
                errorList.add(err);
                result.success = false;
            }
        }

        wb.close();
        result.data = Map.of("errors", errorList);
        return result;
    }


    private String getString(Cell cell){
        if (cell == null) return "";
        return switch (cell.getCellType()) {
            case STRING -> cell.getStringCellValue().strip();
            case NUMERIC -> String.valueOf((long) cell.getNumericCellValue());
            default -> "";
        };
    }

    private int parseInt(Cell cell) {
        if (cell == null) return 0;
        try {
            if (cell.getCellType() == CellType.NUMERIC) {
                return (int) cell.getNumericCellValue();
            } else if (cell.getCellType() == CellType.STRING) {
                return Integer.parseInt(cell.getStringCellValue().replaceAll(",", "").trim());
            }
        } catch (Exception ignored) {}
        return 0;
    }

    private boolean isRowEmpty(Row row) {
        if (row == null) return true;
        for (int c = 0; c < row.getLastCellNum(); c++) {
            Cell cell = row.getCell(c);
            if (cell != null && cell.getCellType() != CellType.BLANK) {
                String val = getString(cell);
                if (val != null && !val.isBlank()) return false;
            }
        }
        return true;
    }

    public Map<String, Object> getInvoicePrint(Integer misnum) throws IOException {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("misnum", misnum);

        String sql = """ 
                SELECT
                	m.icercorpnum,
                	m.icercorpnm,
                	m.icerceonm,
                	m.iceraddr,
                	m.icerbiztype,
                	m.icerbizclass,
                	m.ivercorpnum,
                	m.ivercorpnm,
                	m.iverceonm,
                	m.iveraddr,
                	m.iverbiztype,
                	m.iverbizclass,
                	m.supplycost,
                    m.taxtotal,
                 	m.totalamt
                FROM tb_salesment m
                WHERE m.misnum = :misnum
                """;

        String detailSql = """ 
            SELECT
                ROW_NUMBER() OVER (ORDER BY sh."ShipDate") AS misseq,
                TO_CHAR(sh."ShipDate", 'MM') AS month,
                   TO_CHAR(sh."ShipDate", 'DD') AS day,
                   sh."ShipDate",
                sh.misnum,
                s."Qty" as qty,
                s."UnitPrice" as unitcost,
                s."Price" as supplycost,
                s."Vat" as tax,
                s."Description" as remark,
                   m."Name" AS name
               FROM
                   shipment_head sh
               JOIN
                   shipment s ON sh.id = s."ShipmentHead_id"
               JOIN
                   material m ON s."Material_id" = m.id
               WHERE
                   sh.misnum = :misnum
               order by sh."ShipDate"
            """;

        String fallbackDetailSql = """
            SELECT
                d.misseq,
                d.itemnm as name,
                d.qty,
                d.unitcost,
                d.supplycost,
                d.taxtotal as tax,
                d.remark,
                SUBSTRING(d.misdate FROM 5 FOR 2) AS month,
                SUBSTRING(d.misdate FROM 7 FOR 2) AS day
            FROM tb_salesdetail d
            WHERE d.misnum = :misnum
            ORDER BY d.misseq
            """;

        Map<String, Object> master = this.sqlRunner.getRow(sql, paramMap);

        // 기존에 shipment_head 에 misnum 을 가지고 있으면 해당 단가등 정보를 가져왔는데,
        // 프린트는 매출등록 이후에 진행하니 tb_salesmentdetail 에서 정보를 가져오게 수정
//        List<Map<String, Object>> detailList = this.sqlRunner.getRows(detailSql, paramMap);
        List<Map<String, Object>> detailList = this.sqlRunner.getRows(fallbackDetailSql, paramMap);

//        if (detailList == null || detailList.isEmpty()) {
//            detailList = this.sqlRunner.getRows(fallbackDetailSql, paramMap);
//        }

        UtilClass.decryptItem(master, "ivercorpnum", 0);

        master.put("icercorpnum", formatIdentifier((String) master.get("icercorpnum")));
        master.put("ivercorpnum", formatIdentifier((String) master.get("ivercorpnum")));
        master.put("detailList", detailList);
        return master;
    }

    private String formatIdentifier(String num) {
        if (num == null) return "";
        num = num.replaceAll("[^0-9]", ""); // 숫자만 추출

        if (num.length() == 10) {
            // 사업자등록번호: 000-00-00000
            return num.substring(0, 3) + "-" + num.substring(3, 5) + "-" + num.substring(5);
        } else if (num.length() == 13) {
            // 주민등록번호: 000000-0000000
            return num.substring(0, 6) + "-" + num.substring(6);
        }
        return num; // 길이 안 맞으면 그대로 반환
    }

}
