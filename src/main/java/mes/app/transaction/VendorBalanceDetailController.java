package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.VendorBalanceDetailService;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@Slf4j
@RestController
@RequestMapping("/api/transaction/vendor_balance_detail")
public class VendorBalanceDetailController {
    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    VendorBalanceDetailService vendorBalanceDetailService;

    // 거래처별 잔액 명세(출금) 리스트 조회
    @DecryptField(columns  = {"accnum"})
    @GetMapping("/read")
    public AjaxResult getEquipmentRunChart(
            @RequestParam(value="srchStartDt", required=false) String start,
            @RequestParam(value="srchEndDt", required=false) String end,
            @RequestParam(value="cboCompany", required=false) String company,
            @RequestParam(value = "spjangcd") String spjangcd,
            HttpServletRequest request) {

        //log.info("거래처잔액 명세(입금 관리) read ---  :start:{}, end:{} ,company:{}, spjangcd:{} ", start_date, end_date, company, spjangcd);

        AjaxResult result = new AjaxResult();

        result.data = vendorBalanceDetailService.getPaymentList(start, end, company,spjangcd);

        return result;
    }
}
