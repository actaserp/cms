package mes.app.definition;

import mes.app.definition.service.material.TradeService;
import mes.domain.entity.Accsubject;
import mes.domain.entity.Bom;
import mes.domain.entity.Trade;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TradeRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;

@RestController
@RequestMapping("/api/definition/trade")
public class TradeController {


    @Autowired
    TradeRepository tradeRepository;

    @Autowired
    TradeService tradeService;

    @RequestMapping("/read")
    public AjaxResult gettradeList(
            @RequestParam(value="searchtradenm" , required=false) String searchtradenm,
            @RequestParam(value="searchioflag" , required=false) String searchioflag,
            @RequestParam(value ="spjangcd") String spjangcd){

        AjaxResult result = new AjaxResult();
        result.data = this.tradeService.gettradeList(searchtradenm,searchioflag,spjangcd);
        return result;
    }


    @PostMapping("/save")
    public AjaxResult savetrade(
            @RequestParam(value="id") String id, // 계정코드
            @RequestParam(value="ioflag" , required=false) String ioflag, // 입출금구분
            @RequestParam(value="tradenm") String tradenm, //거래구분명
            @RequestParam(value="acccd" , required=false) String acccd, //계정코드
            @RequestParam(value="reacccd", required=false) String reacccd, //상대계정
            @RequestParam(value="remark" , required=false) String remark, // 비고
            @RequestParam(value ="spjangcd") String spjangcd,
            Authentication auth
    ) {

        User user = (User)auth.getPrincipal();
        AjaxResult result = new AjaxResult();

        Trade trade = null;

        if (id != null && !id.trim().isEmpty()) {
            trade = this.tradeRepository.getTradeById(Integer.parseInt(id));
            trade.setId(Integer.valueOf(id));
        } else {
            trade = new Trade();
        }

        trade.setIoflag(ioflag); //입출구분
        trade.setTradenm(tradenm); // 거래구분명
        trade.setAcccd(acccd); // 계정코드
        trade.setReacccd(reacccd); // 상대계정
        trade.setRemark(remark); // 비고
        trade.setSpjangcd(spjangcd);

        this.tradeRepository.save(trade);
        result.data = trade;

        return result;

    }

    @GetMapping("/detail")
    public AjaxResult getTradeDetail(
            @RequestParam("id") String id,
            @RequestParam(value ="spjangcd") String spjangcd,
            HttpServletRequest request) {

        int idx = Integer.parseInt(id);
        Map<String, Object> item = this.tradeService.getTradeDetail(idx,spjangcd);

        AjaxResult result = new AjaxResult();
        result.data = item;

        return result;
    }

    @PostMapping("/delete")
    public AjaxResult deleteTrade(@RequestParam(value="id") String id ) {
        this.tradeRepository.deleteById(Integer.valueOf(id));
        AjaxResult result = new AjaxResult();
        return result;
    }


    @GetMapping("/search_acc")
    public AjaxResult getAccSearchList(
            @RequestParam(value="searchCode", required=false) String code,
            @RequestParam(value="searchName", required=false) String name,
            @RequestParam(value ="spjangcd") String spjangcd
    ) {

        AjaxResult result = new AjaxResult();
        result.data = this.tradeService.getAccSearchitem(code,name,spjangcd);
        return result;
    }

}
