package mes.app.transaction;


import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.TransactionInputService;
import mes.app.util.UtilClass;
import mes.domain.dto.BankTransitDto;
import mes.domain.model.AjaxResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.validation.BindingResult;
import org.springframework.web.bind.annotation.*;

import javax.sql.DataSource;
import javax.validation.Valid;
import java.util.*;

@RestController
@RequestMapping("/api/transaction/input")
@Slf4j
public class TransactionInputController {

    @Autowired
    TransactionInputService transactionInputService;

    @Autowired
    DataSource dataSource;


    @DecryptField(columns = {"accountnumber", "paymentpw", "onlinebankpw", "viewpw"}, masks = {0, 2, 2, 2})
    @GetMapping("/registerAccount")
    public AjaxResult registerAccount(@RequestParam String spjangcd){

        AjaxResult result = new AjaxResult();

        result.data = transactionInputService.getAccountList(spjangcd);

        Map<String, Object> status = new LinkedHashMap<>();


        return  result;

    }

    @DecryptField(columns = {"account", "clientName"}, masks = 0)
    @GetMapping("/history")
    public AjaxResult TransactionHistory(@RequestParam String searchfrdate,
                                         @RequestParam String searchtodate,
                                         @RequestParam String tradetype,
                                         @RequestParam String spjangcd,
                                         @RequestParam(required = false) String accountNameHidden,
                                         @RequestParam(required = false) String cltflag,
                                         @RequestParam(required = false) String cboCompanyHidden) throws InterruptedException {
        long start = System.currentTimeMillis();

        AjaxResult result = new AjaxResult();

        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");

        Integer parsedAccountId = null;
        if(accountNameHidden != null && !accountNameHidden.isEmpty()){
            parsedAccountId = UtilClass.parseInteger(accountNameHidden);
        }

        Integer parsedCompanyId = null;
        if(cboCompanyHidden != null && !cboCompanyHidden.isEmpty()){
            parsedCompanyId = UtilClass.parseInteger(cboCompanyHidden);
        }

        Map<String, Object> param = new HashMap<>();

        param.put("searchfrdate", searchfrdate);
        param.put("searchtodate", searchtodate); // searchtodate가 null이더라도 문제없이 추가됨
        param.put("parsedAccountId", parsedAccountId);
        param.put("parsedCompanyId", parsedCompanyId);
        param.put("spjangcd", spjangcd);
        param.put("cltflag", cltflag);
        param.put("tradetype", tradetype);

        result.data = transactionInputService.getTransactionHistory(param);


        long end = System.currentTimeMillis();
        System.out.println("끝남시간: " + end);
        System.out.println("[/history] 처리 시간: " + (end - start) + " ms");
        return result;
    }

    @PostMapping("/transactionForm")
    public AjaxResult transactionForm(@Valid @RequestBody BankTransitDto data,
                                      BindingResult bindingResult){

        AjaxResult result = new AjaxResult();

        if(bindingResult.hasErrors()){
            result.success = false;
            result.message = bindingResult.getAllErrors().get(0).getDefaultMessage();
            return result;
        }
        try{
            transactionInputService.saveBankTransit(data);
            result.success = true;
            result.message = "저장하였습니다.";
        }catch(Exception e){
            result.success = false;
            result.message = "오류가 발생하였습니다.";

            return result;
        }

        return  result;
    }

    @PostMapping("/AccountEdit")
    public AjaxResult AccountEdit(@RequestBody Object list){

        AjaxResult result = new AjaxResult();

        try {
            transactionInputService.editAccountList((List<Map<String, Object>>)list);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        result.message = "수정되었습니다.";
        return  result;

    }

    @PostMapping("/edit")
    public AjaxResult transactionEdit(@RequestBody Object list){

        AjaxResult result = transactionInputService.editBankTransit(list);
        return result;
    }

    @DecryptField(columns = {"accountNumber", "clientName"}, masks = 0)
    @GetMapping("/searchDetail")
    public AjaxResult searchTransactionDetail(@RequestParam String companyId,
                                              @RequestParam String cltflag,
                                              @RequestParam String searchfrdate,
                                              @RequestParam String searchtodate,
                                              @RequestParam String spjangcd){
        AjaxResult result = new AjaxResult();

        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");

        result.data = transactionInputService.searchDetail(UtilClass.parseInteger(companyId), cltflag, searchfrdate,  searchtodate, spjangcd);

        return  result;
    }

    @PostMapping("/delete")
    public AjaxResult transactionDelete(@RequestParam String idList){

        AjaxResult result = new AjaxResult();

        List<Integer> parsedidList = Arrays.stream(idList.split(","))
                        .map(Integer::parseInt)
                                .toList();

        transactionInputService.deleteBanktransit(parsedidList);

        result.message = "삭제되었습니다.";
        return  result;

    }


}
