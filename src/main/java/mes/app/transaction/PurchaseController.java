package mes.app.transaction;


import mes.app.aspect.DecryptField;
import mes.app.transaction.service.PurchaseService;
import mes.domain.model.AjaxResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/purchase/list")
public class PurchaseController {

    private final PurchaseService purchaseService;

    public PurchaseController(PurchaseService purchaseService) {
        this.purchaseService = purchaseService;
    }


    @GetMapping("/search")
    public AjaxResult searchList(
            @RequestParam String spjangcd,
            @RequestParam String searchfrdate,
            @RequestParam String searchtodate,
            @RequestParam String purchase_type,
            @RequestParam String cltcd
            //@RequestParam String taxtype
    ){
        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");



        AjaxResult result = new AjaxResult();

        Map<String, Object> paramSet = new HashMap<>();
        paramSet.put("searchfrdate", searchfrdate);
        paramSet.put("searchtodate", searchtodate);
        paramSet.put("spjangcd", spjangcd);
        paramSet.put("cltcd", cltcd);
        //paramSet.put("taxtype", taxtype);
        paramSet.put("misgubun", purchase_type);

        List<Map<String, Object>> list = purchaseService.getList(paramSet);

        //salesListService.bindEnumLabels(list);

        result.data = list;

        return result;
    }

    @DecryptField(columns = {"saupnum"}, masks = 3)
    @GetMapping("/search2")
    public AjaxResult searchList2(
            @RequestParam String spjangcd,
            @RequestParam String searchfrdate2,
            @RequestParam String searchtodate2,
            @RequestParam String purchase2,
            @RequestParam String cltcd2
            //@RequestParam String taxtype2
    ){
        searchfrdate2 = searchfrdate2.replaceAll("-", "");
        searchtodate2 = searchtodate2.replaceAll("-", "");

        AjaxResult result = new AjaxResult();

        Map<String, Object> paramSet = new HashMap<>();
        paramSet.put("searchfrdate", searchfrdate2);
        paramSet.put("searchtodate", searchtodate2);
        paramSet.put("spjangcd", spjangcd);
        paramSet.put("cltcd", cltcd2);
        //paramSet.put("taxtype", taxtype2);
        paramSet.put("misgubun", purchase2);

        List<Map<String, Object>> list = purchaseService.getList2(paramSet);

        result.data = list;
        return result;
    }
}
