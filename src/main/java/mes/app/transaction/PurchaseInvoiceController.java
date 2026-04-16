package mes.app.transaction;

import mes.app.aspect.DecryptField;
import mes.app.transaction.service.PurchaseInvoiceService;
import mes.domain.entity.Material;
import mes.domain.model.AjaxResult;
import mes.domain.repository.CompanyRepository;
import mes.domain.repository.MaterialRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.*;

@RestController
@RequestMapping("/api/tran/purchase")
public class PurchaseInvoiceController {
	
	@Autowired
	private PurchaseInvoiceService purchaseInvoiceService;
    @Autowired
    private CompanyRepository companyRepository;
    @Autowired
    private MaterialRepository materialRepository;

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

	// 검색
	@DecryptField(columns = {"incardnum", "paycltnm", "cltnm"}, masks = {0, 0, 0})
	@GetMapping("/read")
	public AjaxResult getInvoiceList(
			@RequestParam(value="invoice_kind", required=false) String invoice_kind,
			@RequestParam(value="start", required=false) String start_date,
			@RequestParam(value="end", required=false) String end_date,
			@RequestParam(value="cboCompany", required=false) Integer cboCompany,
			@RequestParam(value="spjangcd", required=false) String spjangcd,
			HttpServletRequest request) {

		start_date = start_date + " 00:00:00";
		end_date = end_date + " 23:59:59";

		Timestamp start = Timestamp.valueOf(start_date);
		Timestamp end = Timestamp.valueOf(end_date);

		List<Map<String, Object>> items = this.purchaseInvoiceService.getList(invoice_kind, cboCompany, start, end, spjangcd);

		AjaxResult result = new AjaxResult();
		result.data = items;

		return result;
	}

	// 세금계산서 저장
	@PostMapping("/invoice_save")
	public AjaxResult saveInvoice(@RequestBody Map<String, Object> form
	, Authentication auth) {

		return purchaseInvoiceService.saveInvoice(form);
	}

	@PostMapping("/invoice_update")
	public AjaxResult updateInvoice(@RequestParam(value="misnum") Integer misnum,
									@RequestParam(value="issuediv") String issuediv,
									Authentication auth) {

		return purchaseInvoiceService.updateinvoice(misnum, issuediv);
	}

	@GetMapping("/invoice_detail")
	public AjaxResult getInvoiceDetail(
			@RequestParam("misnum") Integer misnum,
			HttpServletRequest request) throws IOException {

		Map<String, Object> item = this.purchaseInvoiceService.getInvoiceDetail(misnum);

		AjaxResult result = new AjaxResult();
		result.data = item;

		return result;
	}

	@PostMapping("/invoice_delete")
	public AjaxResult deleteSalesment(@RequestBody List<Map<String, String>> deleteList) {

		return purchaseInvoiceService.deleteInvoicement(deleteList);
	}

	@PostMapping("/invoice_copy")
	public AjaxResult copyInvoice(@RequestBody List<Map<String, String>> copyList) {

		return purchaseInvoiceService.copyInvoice(copyList);
	}

}
