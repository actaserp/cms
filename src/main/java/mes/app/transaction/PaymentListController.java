package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.PaymentListService;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/transaction/payment_list")
public class PaymentListController {
    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    PaymentListService paymentListService;

    // 지급현황 리스트 조회
    @DecryptField(columns  = {"accnum"})
    @GetMapping("/read")
    public AjaxResult getEquipmentRunChart(
        @RequestParam(value="cboDepositType", required=false) String depositType,
        @RequestParam(value="srchStartDt", required=false) String start_date,
        @RequestParam(value="srchEndDt", required=false) String end_date,
        @RequestParam(value = "cboCompany", required=false) String company,
        @RequestParam(value = "txtDescription", required = false) String txtDescription,
        @RequestParam(value = "AccountName", required = false) String AccountName,
        @RequestParam(value = "txtEumnum", required = false) String txtEumnum,
        @RequestParam(value = "spjangcd")String spjangcd,
        HttpServletRequest request) {
        /*log.info("지급현황 read : depositType:{}, start_date:{}, end_date:{},company:{}, txtDescription:{} ,AccountName:{}, txtEumnum:{}, spjangcd:{}",
            depositType, start_date, end_date, company, txtDescription, AccountName, txtEumnum, spjangcd);*/
        start_date = start_date + " 00:00:00";
        end_date = end_date + " 23:59:59";

        Timestamp start = Timestamp.valueOf(start_date);
        Timestamp end = Timestamp.valueOf(end_date);

        List<Map<String, Object>> items  = this.paymentListService.getPaymentList(depositType,start, end, company, txtDescription,AccountName, txtEumnum, spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }
}
