package mes.app.transaction;

import com.fasterxml.jackson.databind.JsonNode;
import com.openhtmltopdf.outputdevice.helper.BaseRendererBuilder;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.SalesInvoiceService;
import mes.config.Settings;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.MaterialRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.multipart.MultipartFile;
import com.openhtmltopdf.pdfboxout.PdfRendererBuilder;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.sql.Timestamp;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
@RequestMapping("/api/tran/sales")
public class SalesInvoiceController {
	
	@Autowired
	private SalesInvoiceService salesInvoiceService;
    @Autowired
    private CompanyRepository companyRepository;
    @Autowired
    private MaterialRepository materialRepository;
	@Autowired
	Settings settings;

	@GetMapping("/shipment_head_list")
	public AjaxResult getShipmentHeadList(
			@RequestParam("srchStartDt") String dateFrom,
			@RequestParam("srchEndDt") String dateTo,
			@RequestParam(value="comp_id", required=false) Integer cltcd
	) {
		
		List<Map<String, Object>> items = this.salesInvoiceService.getShipmentHeadList(dateFrom,dateTo, cltcd);

		AjaxResult result = new AjaxResult();
		result.data = items;
		
		return result;
	}

	@GetMapping("/get_material")
	public AjaxResult getMaterialName(
			@RequestParam("material_id") Integer id
	) {

		Material item = materialRepository.getMaterialById(id);

		Map<String, Object> data = new HashMap<>();
		data.put("material_name", item.getName());
		data.put("spec", item.getStandard1());

		AjaxResult result = new AjaxResult();
		result.data = data;
		return result;
	}


	// 공급자(사업장) 정보 가져오기
	@GetMapping("/invoicer_read")
	public AjaxResult getInvoicerDatail(
			@RequestParam("spjangcd") String spjangcd
	) {

		Map<String, Object> item = this.salesInvoiceService.getInvoicerDatail(spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	// 공급받는자 휴폐업 조회
	@PostMapping("/invoicee_check")
	public AjaxResult invoiceeCheck(
			@RequestParam("b_no") String bno, // 사업자 번호
			@RequestParam("compid") Integer compid, // company id
			Authentication auth) {

		AjaxResult result = new AjaxResult();

		try {
			JsonNode data = salesInvoiceService.validateSingleBusiness(bno);

			if (data == null) {
				result.success = false;
				result.message = "계속사업자가 아니므로 거래중지 처리하시겠습니까?";
				result.code = "STOP_CONFIRM"; // 추가
				return result;
			}

			String statusCode = data.path("b_stt_cd").asText();
			String statusText = data.path("b_stt").asText();
			String taxTypeText = data.path("tax_type").asText();

			if ("01".equals(statusCode)) {
				result.success = true;
				result.data = data; // 단건 결과 JSON 문자열로 반환
			} else {
				Company company = companyRepository.getCompanyById(compid);
				company.setRelyn("1");
				companyRepository.save(company);

				if (statusText == null || statusText.isBlank()) {
					result.success = false;
					result.message = taxTypeText + "\n거래중지 처리되었습니다.";
				} else {
					result.success = false;
					result.message = "사업자 상태: " + statusText + " 거래중지 처리되었습니다.";
				}
			}

		} catch (Exception e) {
			result.success = false;
			result.message = "사업자 진위 확인 실패: " + e.getMessage();
		}

		return result;
	}

	@PostMapping("/invoicee_stop")
	public AjaxResult stopInvoicee(@RequestParam("compid") Integer compid) {
		AjaxResult result = new AjaxResult();
		try {
			Company company = companyRepository.getCompanyById(compid);
			company.setRelyn("1");
			companyRepository.save(company);
			result.success = true;
			result.message = "거래중지 처리되었습니다.";
		} catch (Exception e) {
			result.success = false;
			result.message = "거래중지 처리 실패: " + e.getMessage();
		}
		return result;
	}

	// 공급받는자 저장
	@PostMapping("/invoicee_save")
	public AjaxResult invoiceeSave(
			@RequestParam Map<String, Object> paramMap,
			Authentication auth) {

		User user = (User) auth.getPrincipal();
		return salesInvoiceService.saveInvoicee(paramMap, user);
	}

	// 검색
	@DecryptField(columns = {"ivercorpnum"}, masks = 6)
	@GetMapping("/read")
	public AjaxResult getInvoiceList(
			@RequestParam(value="invoice_kind", required=false) String invoice_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="cboCompany", required=false) Integer cboCompany,
			@RequestParam(value="cboStatecode", required=false) Integer cboStatecode,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.salesInvoiceService.getList(invoice_kind, cboStatecode, cboCompany, start, end, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 세금계산서 저장
	@PostMapping("/invoice_save")
	public AjaxResult saveInvoice(@RequestBody Map<String, Object> form
	, Authentication auth) {
		User user = (User) auth.getPrincipal();
		AjaxResult result = new AjaxResult();
		String invoiceeType = (String) form.get("InvoiceeType");

		if ("사업자".equals(invoiceeType)) {

			// 1. InvoiceeID 없는 경우 → 사업자번호로 조회
			String corpNum = (String) form.get("InvoiceeCorpNum");

			// 2. 사업자번호 유효성 체크
			if (salesInvoiceService.validateSingleBusiness(corpNum) == null) {
				result.success = false;
				result.message = "휴/폐업 사업자번호입니다.\n공급받는자 등록번호를 확인해주세요.";
				return result;
			}

			// 3. company 테이블에 존재 확인
			Optional<Company> comp = companyRepository.findByBusinessNumber(corpNum);
			if (comp.isPresent()) {
				form.put("InvoiceeID", comp.get().getId());
			} else {
				// 4. 없으면 신규 등록
				AjaxResult compResult = salesInvoiceService.saveInvoicee(form, user);
				if (!compResult.success) {
					return compResult; // 에러 바로 리턴
				}
				Company newComp = (Company) compResult.data;
				form.put("InvoiceeID", newComp.getId());
			}
		}


		// 수정세금계산서 신규 생성
		if ("true".equals(String.valueOf(form.get("newModifiedInvoice")))) {
			return salesInvoiceService.saveModifiedInvoice(form);
		} else {
			return salesInvoiceService.saveInvoice(form);
		}
	}

	@PostMapping("/invoice_update")
	public AjaxResult updateInvoice(@RequestParam(value="misnum") Integer misnum,
									@RequestParam(value="issuediv") String issuediv,
									Authentication auth) {

		return salesInvoiceService.updateinvoice(misnum, issuediv);
	}

	@PostMapping("/save_invoice_pdf")
	public ResponseEntity<?> saveInvoicePdf(@RequestBody Map<String, String> body) {
		String html = body.get("html");
		String misnum = body.get("misnum");

		String basePath = settings.getProperty("file_temp_upload_path");
		if (basePath == null) {
			return ResponseEntity.status(500).body("PDF 경로 설정이 누락되었습니다.");
		}

		File dir = new File(basePath);
		if (!dir.exists()) dir.mkdirs();

		String today = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
		String filePath = basePath + "TAX-" + today + "-" + misnum + ".pdf";

		try (OutputStream os = new FileOutputStream(filePath)) {
			PdfRendererBuilder builder = new PdfRendererBuilder();

			// Windows에서 맑은 고딕 폰트 등록
			builder.useFont(
					new File("C:/Windows/Fonts/malgun.ttf"),
					"Malgun Gothic",
					400,
					BaseRendererBuilder.FontStyle.NORMAL,
					true
			);

			builder.withHtmlContent(html, null);
			builder.toStream(os);
			builder.run();
			os.flush();

		} catch (Exception e) {
			return ResponseEntity.status(500).body("PDF 생성 실패");
		}

		return ResponseEntity.ok(Map.of("success", true, "message", "PDF 생성 완료"));

	}


	@PostMapping("/invoice_issue")
	public AjaxResult issueInvoice(@RequestBody List<Map<String, String>> issueList) {

		return salesInvoiceService.issueInvoice(issueList);
	}

	@GetMapping("/invoice_detail")
	public AjaxResult getInvoiceDetail(
			@RequestParam("misnum") Integer misnum,
			HttpServletRequest request) throws IOException {

		Map<String, Object> item = this.salesInvoiceService.getInvoiceDetail(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@PostMapping("/invoice_delete")
	public AjaxResult deleteSalesment(@RequestBody List<Map<String, String>> deleteList) {

		return salesInvoiceService.deleteSalesment(deleteList);
	}

	@PostMapping("/cancel_issue")
	public AjaxResult cancelIssue(@RequestBody List<Map<String, String>> cancelList) {

		return salesInvoiceService.cancelIssue(cancelList);
	}

	@PostMapping("/re_message")
	public AjaxResult reMessage(@RequestBody List<Map<String, String>> invoiceList) {

		return salesInvoiceService.reMessage(invoiceList);
	}

	@PostMapping("/issue_delete")
	public AjaxResult deleteInvoice(@RequestBody List<Map<String, String>> delList) {

		return salesInvoiceService.deleteInvoice(delList);
	}

	@PostMapping("/upload_save")
	public AjaxResult saveInvoiceBulkData(
			@RequestParam(value="upload_file") MultipartFile upload_file,
			String spjangcd, Authentication auth) throws FileNotFoundException, IOException  {

		User user = (User) auth.getPrincipal();
		return salesInvoiceService.saveInvoiceBulkData(upload_file, spjangcd, user);
	}

	@PostMapping("/invoice_copy")
	public AjaxResult copyInvoice(@RequestBody List<Map<String, String>> copyList) {

		return salesInvoiceService.copyInvoice(copyList);
	}

	@GetMapping("/invoice_print")
	public AjaxResult getInvoicePrint(
			@RequestParam("misnum") Integer misnum,
			HttpServletResponse response) throws IOException {

		Map<String, Object> item = this.salesInvoiceService.getInvoicePrint(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

}
