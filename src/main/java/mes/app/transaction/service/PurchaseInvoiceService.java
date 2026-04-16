package mes.app.transaction.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.popbill.api.*;
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
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.multipart.MultipartFile;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigDecimal;
import java.net.URI;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PurchaseInvoiceService {

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    private CompanyRepository companyRepository;

    @Autowired
    private TB_InvoicementRepository tb_invoicementRepository;

    @Autowired
    private TB_InvoiceDetailRepository tb_invoiceDetailRepository;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Value("${invoice.api.key}")
    private String invoiceeCheckApiKey;

    @Autowired
    private RestTemplate restTemplate;

    @Autowired
    private ObjectMapper jacksonObjectMapper;


    @Autowired
    private SysCodeRepository sysCodeRepository;

    @Autowired
    Settings settings;

    public List<Map<String, Object>> getList(String invoice_kind, Integer cboCompany, Timestamp start, Timestamp end, String spjangcd) {

        MapSqlParameterSource dicParam = new MapSqlParameterSource();
        dicParam.addValue("invoice_kind", invoice_kind);
        dicParam.addValue("cboCompany", cboCompany);
        dicParam.addValue("start", start);
        dicParam.addValue("end", end);
        dicParam.addValue("spjangcd", spjangcd);

        String sql = """
                WITH detail_summary AS (
                    SELECT
                        misnum,
                        MIN(itemnm) AS first_itemnm,
                        COUNT(*) AS item_count
                    FROM tb_invoicedetail
                    GROUP BY misnum
                ),
                clt_unified AS (
                    SELECT id, '0' AS flag, "Code", "Name" AS name, NULL AS accnum, NULL AS accname, NULL AS cardnum, NULL AS cardnm FROM company
                    UNION ALL
                    SELECT id, '1' AS flag, "Code", "Name", NULL, NULL, NULL, NULL FROM person
                    UNION ALL
                    SELECT accid AS id, '2', NULL, NULL, accnum, accname, NULL, NULL FROM tb_account
                    UNION ALL
                    SELECT id, '3', NULL, NULL, NULL, null, cardnum, cardnm FROM tb_iz010
                ),
                payclt_unified AS (
                    SELECT id, '0' AS flag, "Code", "Name" AS name, NULL AS accnum, NULL AS accname, NULL AS cardnum, NULL AS cardnm FROM company
                    UNION ALL
                    SELECT id, '1', "Code", "Name", NULL, NULL, NULL, NULL FROM person
                    UNION ALL
                    SELECT accid AS id, '2', NULL, NULL, accnum, accname, NULL, NULL FROM tb_account
                    UNION ALL
                    SELECT id, '3', NULL, NULL, NULL, null, cardnum, cardnm FROM tb_iz010
                )
                                
                SELECT
                    TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS misdate,
                    m.misnum,
                    m.misgubun,
                    purchase_type_code."Value" AS misgubun_name,
                    m.paycltcd,
                    m.cltcd,
                    COALESCE(cu.name, cu.accnum, cu.cardnum) AS cltnm,
                	COALESCE(cu.accname, cu.cardnm, cu."Code") AS cltnmsub,
                    COALESCE(pcu.name, pcu.accnum, pcu.cardnum) AS paycltnm,
                	COALESCE(pcu.accname, pcu.cardnm, pcu."Code") AS paycltnmsub,
                    m.totalamt,
                    m.supplycost,
                    m.taxtotal,
                    m.title,
                    m.deductioncd,
                    de.name AS dedunm,
                    m.depart_id,
                    dp."Name" AS dpName,
                    m.card_id,
                    iz.cardnum AS incardnum,
                    CASE
                        WHEN ds.item_count > 1 THEN ds.first_itemnm || ' 외 ' || (ds.item_count - 1) || '개'
                        WHEN ds.item_count = 1 THEN ds.first_itemnm
                        ELSE NULL
                    END AS item_summary
                                
                FROM tb_invoicement m
                LEFT JOIN detail_summary ds ON m.misnum = ds.misnum
                LEFT JOIN clt_unified cu ON m.cltcd = cu.id AND m.cltflag = cu.flag
                LEFT JOIN payclt_unified pcu ON m.paycltcd = pcu.id AND m.paycltflag = pcu.flag
                LEFT JOIN vat_deduction_type de ON m.deductioncd = de.code
                LEFT JOIN depart dp ON m.depart_id = dp.id
                LEFT JOIN tb_iz010 iz ON m.card_id = iz.id
                LEFT JOIN sys_code purchase_type_code ON purchase_type_code."CodeType" = 'purchase_type'
                    AND purchase_type_code."Code" = m.misgubun
                WHERE 1=1
                and m.spjangcd = :spjangcd 
                     """; // 조건은 아래에서 붙임

        if (invoice_kind != null && !invoice_kind.isEmpty()) {
            sql += " and m.misgubun = :invoice_kind ";
        }

        if (cboCompany != null) {
            sql += " and m.cltcd = :cboCompany ";
        }

        if (start != null && end != null) {
            sql += " and to_date(m.misdate, 'YYYYMMDD') between :start and :end ";
        }

        return this.sqlRunner.getRows(sql, dicParam);
    }

    @Transactional
    public AjaxResult saveInvoice(@RequestBody Map<String, Object> form) {

        AjaxResult result = new AjaxResult();
        try {

            // 인보이스 저장
            saveInvoiceInternal(form);


            result.success = true;
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

    private TB_Invoicement saveInvoiceInternal(Map<String, Object> form) {
        // 1. 기본 키 생성
        Integer misnum = parseInt(form.get("misnum"));
        boolean isUpdate = misnum != null;

        TB_Invoicement invoicement;

        if (isUpdate) {
            invoicement = tb_invoicementRepository.findById(misnum)
                    .orElseThrow(() -> new RuntimeException("수정할 데이터가 없습니다."));
        } else {
            invoicement = new TB_Invoicement();
        }

        String misdate = sanitizeNumericString(form.get("writeDate"));
        invoicement.setMisdate(misdate);
        invoicement.setMisgubun((String)form.get("purchase_type"));
        invoicement.setCltcd(parseInt(form.get("InvoicerID")));
        invoicement.setCltflag((String) form.get("cltflag"));
        invoicement.setPaycltflag((String) form.get("paycltflag"));
        invoicement.setPaycltcd(parseInt(form.get("PaymentCorpID")));
        invoicement.setTitle((String)form.get("title"));
        invoicement.setDeductioncd((String) form.get("tax_codeHidden"));
        invoicement.setDepart_id(parseInt(form.get("att_departHidden")));
        invoicement.setCard_id(parseInt(form.get("card_codeHidden")));
        invoicement.setTitle((String)form.get("title"));

        BigDecimal totalAmount = parseMoney(form.get("TotalAmount"));
        if (totalAmount != null) {
            invoicement.setTotalamt(totalAmount.intValue()); // 합계금액
        }
        invoicement.setSupplycost(parseIntSafe(form.get("SupplyCostTotal"))); // 총 공급가액
        invoicement.setTaxtotal(parseIntSafe(form.get("TaxTotal"))); // 총 세액
        invoicement.setRemark1((String) form.get("Remark1")); // 비고1

        Object remark2 = form.get("Remark2");
        if (remark2 != null && !remark2.toString().trim().isEmpty()) {
            invoicement.setRemark2(remark2.toString().trim()); // 비고2
        }

        Object remark3 = form.get("Remark3");
        if (remark3 != null && !remark3.toString().trim().isEmpty()) {
            invoicement.setRemark3(remark3.toString().trim()); // 비고3
        }

        if (isUpdate) {
            tb_invoiceDetailRepository.deleteByMisnum(misnum);   // 현재 PK로 삭제
        }

        invoicement.setSpjangcd((String) form.get("spjangcd"));
        TB_Invoicement saved = tb_invoicementRepository.save(invoicement);

        // 3. 상세 목록 매핑
        int serialIndex = 1;
        List<TB_InvoiceDetail> details = new ArrayList<>();

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

            TB_InvoiceDetail detail = new TB_InvoiceDetail();
            detail.setId(new TB_InvoiceDetailId(saved.getMisnum(), serialNum));
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

            detail.setArtcd((String) form.get(prefix+ ".ExpenseId"));
            detail.setAcccd((String) form.get(prefix+ ".AccountId"));
            detail.setProjcd((String) form.get(prefix + ".ProjectId"));

            String purchaseDT = (String) form.get(prefix + ".PurchaseDT");
            if (purchaseDT != null && purchaseDT.length() == 4) {
                String fullPurchaseDT = misdate.substring(0, 4) + purchaseDT;
                detail.setPurchasedt(fullPurchaseDT);
            } else {
                detail.setPurchasedt(null);
            }

            detail.setInvoicement(saved);
            details.add(detail);

            i++;
        }

        saved.getDetails().clear();
        saved.getDetails().addAll(details);

        return tb_invoicementRepository.save(saved);

    }

    private Throwable getRootCause(Throwable throwable) {
        Throwable cause = throwable;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause;
    }

    public Map<String, Object> getInvoiceDetail(Integer misnum) throws IOException {
        MapSqlParameterSource paramMap = new MapSqlParameterSource();
        paramMap.addValue("misnum", misnum);

        String sql = """ 
                SELECT
                    TO_CHAR(TO_DATE(m.misdate, 'YYYYMMDD'), 'YYYY-MM-DD') AS "writeDate",
                    m.misnum,
                    m.misgubun as "purchase_type",
                    purchase_type_code."Value" AS misgubun_name,  -- fn_code_name 제거
                    m.paycltcd as "PaymentCorpID",
                    m.cltcd as "InvoicerID",
                    case
                         when m.cltflag = '0' then c."Name"
                         when m.cltflag = '1' then p."Name"
                         when m.cltflag = '2' then d.accnum
                         when m.cltflag = '3' then i.cardnum
                         ELSE NULL
                    END as "InvoicerCorpName",
                    case
                         when m.paycltflag = '0' then c2."Name"
                         when m.paycltflag = '1' then p2."Name"
                         when m.paycltflag = '2' then d2.accnum
                         when m.paycltflag = '3' then i2.cardnum
                         ELSE NULL
                    END as "PaymentCorpName",
                    m.title,
                	m.deductioncd as "tax_codeHidden",
                	de.name as "tax_code",
                	m.depart_id as "att_departHidden",
                	dp."Name" AS "att_depart",
                	m.card_id as "card_codeHidden",
                	iz.cardnum as "card_code",
                	
                	m.remark1 AS "Remark1",
                	m.remark2 AS "Remark2",
                	m.remark3 AS "Remark3",
                	
                    m.supplycost AS "SupplyCostTotal",
                	m.taxtotal AS "TaxTotal"
                 
                FROM tb_invoicement m
                   
                LEFT JOIN vat_deduction_type de
                   ON m.deductioncd = de.code
                        
                LEFT JOIN depart dp
                   ON m.depart_id = dp.id
                        
                LEFT JOIN tb_iz010 iz
                   ON m.card_id = iz.id
                   
                LEFT JOIN sys_code purchase_type_code
                   ON purchase_type_code."CodeType" = 'purchase_type'
                   AND purchase_type_code."Code" = m.misgubun
                   
                left join company c on c.id = m.cltcd
                left join person p on p.id = m.cltcd
                left join tb_account d on d.accid = m.cltcd
                left join tb_iz010 i on i.id = m.cltcd
                left join company c2 on c2.id = m.paycltcd
                left join person p2 on p2.id = m.paycltcd
                left join tb_account d2 on d2.accid = m.paycltcd
                left join tb_iz010 i2 on i2.id = m.paycltcd
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
                	 d.artcd AS "ExpenseId",
                	 ex.artnm AS "ExpenseNm",
                	 d.acccd AS "AccountId",
                	 ac.accnm as "AccountNm",
                	 d.projcd AS "ProjectId",
                	 pj.projnm as "ProjectNm"
                	 
                	 
                 FROM tb_invoicedetail d
                 
                 LEFT JOIN tb_ca648 ex
                    ON ex.artcd = d.artcd
                 
                 LEFT JOIN tb_accsubject ac
                    ON ac."acccd" = d.acccd
                 
                 LEFT JOIN TB_DA003 pj
                    ON pj."projno" = d.projcd
                 
                 
                 WHERE d.misnum = :misnum
                 ORDER BY d.misseq::int asc
                """;

        Map<String, Object> master = this.sqlRunner.getRow(sql, paramMap);
        List<Map<String, Object>> detailList = this.sqlRunner.getRows(detailSql, paramMap);

        UtilClass.decryptItem(master, "card_code", 0);
        UtilClass.decryptItem(master, "InvoicerCorpName", 0);
        UtilClass.decryptItem(master, "PaymentCorpName", 0);

        master.put("detailList", detailList);
        return master;
    }

    @Transactional
    public AjaxResult deleteInvoicement(List<Map<String, String>> deleteList) {
        AjaxResult result = new AjaxResult();

        if (deleteList == null || deleteList.isEmpty()) {
            result.success = false;
            result.message = "삭제할 데이터가 없습니다.";
            return result;
        }

        List<Integer> idList = deleteList.stream()
                .map(item -> Integer.parseInt(item.get("misnum")))
                .toList();


        deleteByInvoicedetailIds(idList);

        tb_invoicementRepository.deleteAllById(idList);

        result.success = true;
        return result;
    }

    public void deleteByInvoicedetailIds(List<Integer> idList) {
        if (idList == null || idList.isEmpty()) return;

        String placeholders = idList.stream()
                .map(id -> "?")
                .collect(Collectors.joining(", "));

        String sql = "DELETE FROM tb_invoicedetail WHERE misnum IN (" + placeholders + ")";
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
    public AjaxResult updateinvoice(Integer misnum, String issuediv) {
        AjaxResult result = new AjaxResult();

        TB_Invoicement sm = tb_invoicementRepository.findById(misnum).orElse(null);
        if (sm == null) {
            result.success = false;
            result.message = "해당 데이터가 존재하지 않습니다.";
            return result;
        }


        tb_invoicementRepository.save(sm);

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
                TB_Invoicement origin = tb_invoicementRepository.findById(misnum).orElseThrow();
                TB_Invoicement copy = new TB_Invoicement();

                BeanUtils.copyProperties(origin, copy,
                        "misnum", "writedate", "misdate",
                        "mgtkey", "orgntscfnum", "orgmgtkey",
                        "modifycd", "statedt", "ntscode", "statecode",
                        "details", "ntscfnum"
                );

                copy.setMisnum(null); // 새 엔티티
                copy.setMisdate(misdate); // 새 날짜만 지정

                // 조건부 상태코드 설정

                TB_Invoicement savedCopy = tb_invoicementRepository.save(copy);

                savedCopy = tb_invoicementRepository.save(savedCopy);

                savedCopy = tb_invoicementRepository.save(savedCopy);

                List<TB_InvoiceDetail> details = tb_invoiceDetailRepository.findByMisnum(misnum);
                int sequence = 1;

                for (TB_InvoiceDetail detail : details) {
                    TB_InvoiceDetail newDetail = new TB_InvoiceDetail();
                    TB_InvoiceDetailId newId = new TB_InvoiceDetailId(savedCopy.getMisnum(), String.valueOf(sequence++));
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
                    newDetail.setArtcd(detail.getArtcd());
                    newDetail.setAcccd(detail.getAcccd());
                    newDetail.setProjcd(detail.getProjcd());

                    tb_invoiceDetailRepository.save(newDetail);
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }
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
