package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.transaction.service.MonthlyPurchaseListService;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;
import java.util.Map;

@Slf4j
@RestController
@RequestMapping("/api/transaction/monthly_purchase_list")
public class MonthlyPurchaseListController {    //월별 매입 현황
    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    MonthlyPurchaseListService monthlyPurchaseListService;

    @GetMapping("/PurchaseDetails")
    public AjaxResult getMonthlyPurchaseList(
        @RequestParam(value="cboYear",required=false) String cboYear,
        @RequestParam(value="cboCompany",required=false) Integer cboCompany,
        @RequestParam(value = "spjangcd") String spjangcd,
        @RequestParam(value = "cltflag") String cltflag
    ) {
//        log.info("월별 매입현황(매입)read : cboYear:{}, cboCompany:{} , spjangcd:{},cltflag:{}", cboYear, cboCompany,spjangcd,cltflag);
        List<Map<String,Object>> items = this.monthlyPurchaseListService.getMonthDepositList(cboYear,cboCompany, spjangcd, cltflag);

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    @GetMapping("/ProvisionRead")
    public AjaxResult getMonthDepositList(
        @RequestParam(value="cboYear",required=false) String cboYear,
        @RequestParam(value="cboCompany",required=false) Integer cboCompany,
        @RequestParam(value = "spjangcd") String spjangcd
    ) {
//        log.info("월별 매입현황(지급) read : cboYear:{}, cboCompany:{} , spjangcd:{} ", cboYear, cboCompany, spjangcd);
        List<Map<String,Object>> items = this.monthlyPurchaseListService.getProvisionList(cboYear,cboCompany, spjangcd);

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    @GetMapping("/PaymentRead")
    public AjaxResult getMonthReceivableList(
        @RequestParam(value="cboYear",required=false) String cboYear,
        @RequestParam(value="cboCompany",required=false) Integer cboCompany,
        @RequestParam(value = "spjangcd") String spjangcd,
        @RequestParam(value = "cltflag") String cltflag
    ) {
//        log.info("월별 매입현황(미지급금) read : cboYear:{}, cboCompany:{} , spjangcd:{}, cltflag:{} ", cboYear, cboCompany,spjangcd, cltflag);
        List<Map<String,Object>> items = this.monthlyPurchaseListService.getMonthPayableList(cboYear,cboCompany, spjangcd, cltflag);

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    @GetMapping("/PurchaseDetail")
    public AjaxResult getPurchaseDetail(
        @RequestParam(value = "depart_id") Integer departId,
        @RequestParam(value="cboYear",required=false) String cboYear,
        @RequestParam(value="cltcd",required=false) Integer cltcd,
        @RequestParam(value = "spjangcd") String spjangcd,
        @RequestParam(value = "cltflag") String cltflag
    ) {
//    log.info("월별 매입현황 상세 read : cboYear:{}, cboCompany:{} , spjangcd:{}, cltflag:{}", cboYear, cltcd,spjangcd, cltflag);
        List<Map<String,Object>> items = this.monthlyPurchaseListService.getPurchaseDetail(cboYear,cltcd, spjangcd, departId, cltflag);

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

    @GetMapping("/PaymentDetail")
    public AjaxResult getPaymentDetail(
        @RequestParam(value="cboYear",required=false) String cboYear,
        @RequestParam(value="cltcd",required=false) Integer cltcd,
        @RequestParam(value = "spjangcd") String spjangcd,
        @RequestParam(value = "cltflag") String cltflag
    ) {
//    log.info("월별 매입현황(지급)__지급 상세내역 read : cboYear:{}, cboCompany:{} , spjangcd:{} ", cboYear, cltcd, spjangcd);
        List<Map<String,Object>> items = this.monthlyPurchaseListService.getPaymentDetail(cboYear,cltcd, spjangcd, cltflag);

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }

}
