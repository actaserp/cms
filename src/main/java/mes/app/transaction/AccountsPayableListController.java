package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.AccountsPayableListService;
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
@RequestMapping("/api/transaction/accounts_payable_list")
public class AccountsPayableListController {
    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    AccountsPayableListService accountsPayableListService;

    // 미지급현황 리스트 조회
    @GetMapping("/read")
    public AjaxResult getEquipmentRunChart(
            @RequestParam(value="srchStartDt", required=false) String start,
            @RequestParam(value="srchEndDt", required=false) String end,
            @RequestParam(value="companyCode", required=false) Integer company,
            @RequestParam(value = "spjangcd") String spjangcd,
            @RequestParam(value = "cltflag") String cltflag,
            HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
//        log.info("미지급금 현황 --- start:{}, end:{} ,company:{}, spjangcd:{}, cltflag:{} ", start, end, company, spjangcd, cltflag);
        result.data = accountsPayableListService.getPayableList(start, end, company,spjangcd, cltflag);

        return result;
    }

    // 미지급현황 상세 리스트 조회
    @DecryptField(columns  = {"accnum"})
    @GetMapping("/DetailList")
    public AjaxResult getEquipmentRunChartDetail(
            @RequestParam(value="srchStartDt", required=false) String start,
            @RequestParam(value="srchEndDt", required=false) String end,
            @RequestParam(value = "code", required=false) String company,
            @RequestParam(value = "spjangcd") String spjangcd,
            @RequestParam(value = "cltflag") String cltflag,
            HttpServletRequest request) {
        AjaxResult result = new AjaxResult();

        result.data = accountsPayableListService.getPayableDetailList(start, end, company,spjangcd, cltflag);

        return result;
    }
}
