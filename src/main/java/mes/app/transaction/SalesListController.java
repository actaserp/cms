package mes.app.transaction;


import lombok.extern.slf4j.Slf4j;
import mes.app.aspect.DecryptField;
import mes.app.transaction.service.SalesListService;
import mes.domain.model.AjaxResult;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@Slf4j
@RequestMapping("/api/sales/list")
public class SalesListController {

    private final SalesListService salesListService;

    public SalesListController(SalesListService salesListService) {
        this.salesListService = salesListService;
    }


    @GetMapping("/search")
    public AjaxResult searchList(
                                 @RequestParam String spjangcd,
                                 @RequestParam String searchfrdate,
                                 @RequestParam String searchtodate,
                                 @RequestParam String sale_type,
                                 @RequestParam String cltcd,
                                 @RequestParam String taxtype
                                 ){
        searchfrdate = searchfrdate.replaceAll("-", "");
        searchtodate = searchtodate.replaceAll("-", "");



        AjaxResult result = new AjaxResult();

        Map<String, Object> paramSet = new HashMap<>();
        paramSet.put("searchfrdate", searchfrdate);
        paramSet.put("searchtodate", searchtodate);
        paramSet.put("spjangcd", spjangcd);
        paramSet.put("cltcd", cltcd);
        paramSet.put("taxtype", taxtype);
        paramSet.put("misgubun", sale_type);

        List<Map<String, Object>> list = salesListService.getList(paramSet);

        //salesListService.bindEnumLabels(list);

        result.data = list;

        return result;
    }

    @DecryptField(columns = {"ivercorpnum"}, masks = 3)
    @GetMapping("/search2")
    public AjaxResult searchList2(
            @RequestParam String spjangcd,
            @RequestParam String searchfrdate2,
            @RequestParam String searchtodate2,
            @RequestParam String sale_type2,
            @RequestParam String cltcd2,
            @RequestParam String taxtype2
    ){
        searchfrdate2 = searchfrdate2.replaceAll("-", "");
        searchtodate2 = searchtodate2.replaceAll("-", "");



        AjaxResult result = new AjaxResult();

        Map<String, Object> paramSet = new HashMap<>();
        paramSet.put("searchfrdate", searchfrdate2);
        paramSet.put("searchtodate", searchtodate2);
        paramSet.put("spjangcd", spjangcd);
        paramSet.put("cltcd", cltcd2);
        paramSet.put("taxtype", taxtype2);
        paramSet.put("misgubun", sale_type2);

        List<Map<String, Object>> list = salesListService.getList2(paramSet);

        result.data = list;
        return result;
    }
}
