package mes.app.approval;

import com.fasterxml.jackson.databind.ObjectMapper;
import mes.app.approval.service.ApprovalService;
import mes.config.Settings;
import mes.domain.entity.User;
import mes.domain.entity.approval.TB_E063;
import mes.domain.entity.approval.TB_E063_PK;
import mes.domain.entity.approval.TB_E064;
import mes.domain.entity.approval.TB_E064_PK;
import mes.domain.model.AjaxResult;
import mes.domain.repository.approval.E063Repository;
import mes.domain.repository.approval.E064Repository;
import mes.domain.repository.approval.E080Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

@RestController
@RequestMapping("/api/approval")
public class ApprovalController {
    @Autowired
    private ApprovalService approvalService;

    @Autowired
    private E063Repository e063Repository;

    @Autowired
    private E064Repository e064Repository;

    @Autowired
    private E080Repository e080Repository;

    @Autowired
    Settings settings;
    // 결재라인등록 그리드 read
    @GetMapping("/read")
    public AjaxResult getList(@RequestParam Map<String, String> params
            , Authentication auth) {
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();
//        Map<String, Object> userInfo = requestService.getUserInfo(username);
        String papercd = params.get("papercd");
        String spjangcd = params.get("spjangcd");
        int personid = Optional.ofNullable(params.get("personid"))
                .filter(v -> !v.isEmpty())
                .map(Integer::parseInt)
                .orElse(0);

        List<Map<String, Object>> items = this.approvalService.getCheckPaymentList(personid, papercd, spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 결재라인 사원 그리드 read
    @GetMapping("/readPapercd")
    public AjaxResult getListPapercd(@RequestParam Map<String, String> params
            , Authentication auth) {
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();
//        Map<String, Object> userInfo = requestService.getUserInfo(username);
        String papercd = params.get("papercd");
        String spjangcd = params.get("spjangcd");

        List<Map<String, Object>> items = this.approvalService.getListPapercd(papercd, spjangcd);
        AjaxResult result = new AjaxResult();
        result.data = items;

        return result;
    }

    // 결재자 옵션 불러오기 (사원선택)
    @GetMapping("/getKcperid")
    public AjaxResult getKcperid(){
        List<Map<String, Object>> items = this.approvalService.getKcperid();

        AjaxResult result = new AjaxResult();
        result.data = items;
        return result;
    }
    // 공통코드 리스트 가져오기
    @GetMapping("/find_parent_id")
    public List<Map<String, Object>> getCommonCodeList(@RequestParam("id") Integer id) {
        return approvalService.findByParentId(id);
    }

    // 유저정보 불러와 input태그 value
    @GetMapping("/getUserInfo")
    public AjaxResult getUserInfo(Authentication auth){
        User user = (User) auth.getPrincipal();
        String username = user.getUsername();
        AjaxResult result = new AjaxResult();
        result.data = approvalService.getUserInfo(username);
        return result;
    }

    // 삭제 메서드
    @PostMapping("/delete")
    public AjaxResult deleteBody(@RequestParam Map<String, String> params,
                                 Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();
        System.out.println("params : " + params);

        // 064table PK - custcd,spjangcd,perid,papercd,no
        TB_E064_PK e064PK = new TB_E064_PK();
        e064PK.setPapercd(params.get("papercd"));
        e064PK.setNo(params.get("no"));
        e064PK.setSpjangcd(params.get("spjangcd"));
        e064PK.setPersonid(Integer.valueOf(params.get("personid")));

        try {
            e064Repository.deleteById(e064PK);
            result.success = true;
            result.message = "삭제하였습니다.";
        }
        catch (Exception e){
            result.success = false;
            result.message = "삭제에 실패하였습니다.";
        }
        return result;
    }
    // 063 삭제메서드
    @PostMapping("/deleteHead")
    public AjaxResult deleteHead(@RequestParam Map<String, String> params,
                                 Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        // 063table PK - spjangcd,personid,papercd
        TB_E063_PK e063PK = new TB_E063_PK();
        e063PK.setPapercd(params.get("papercd"));
        e063PK.setSpjangcd(params.get("spjangcd"));
        e063PK.setPersonid(Integer.valueOf(params.get("personid")));

        try {
            e063Repository.deleteById(e063PK);
            result.success = true;
            result.message = "삭제하였습니다.";
        }
        catch (Exception e){
            result.success = false;
            result.message = "삭제에 실패하였습니다.";
        }
        return result;
    }
    // 결재라인 등록
    @PostMapping("/save")
    public AjaxResult saveOrder(@RequestParam Map<String, String> params,
                                Authentication auth) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        AjaxResult result = new AjaxResult();
        TB_E063_PK headpk = new TB_E063_PK();
        TB_E063 head = new TB_E063();
        TB_E064_PK bodypk = new TB_E064_PK();
        TB_E064 body = new TB_E064();
        LocalDateTime createdAt = LocalDateTime.now();
        String indate = createdAt.format(DateTimeFormatter.ofPattern("yyyyMMdd"));

        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        Integer personid = Integer.valueOf(params.get("personid"));
        String spjangcd = params.get("spjangcd");
        String papercd = params.get("papercd");
        String gubun = params.get("gubun");
        String no = params.get("no");
        if(no != null && !no.isEmpty()) {
            no = params.get("no");
        }else{
            no = getNextNoForKey(spjangcd, personid, papercd);
        }
        String seq = params.get("seq");
        Integer kcpersonid = Integer.valueOf(params.get("kcpersonid"));

        // 063 테이블 선언
        headpk.setPersonid(personid);
        headpk.setPapercd(papercd);
        headpk.setSpjangcd(spjangcd);

        head.setId(headpk);

        // 064테이블 선언
//        bodypk.setCustcd((String) userInfo.get("custcd"));
//        bodypk.setPerid(params.get("perid"));
        bodypk.setSpjangcd(spjangcd);
        bodypk.setPapercd(papercd);
        bodypk.setNo(no);
        bodypk.setPersonid(personid);

        body.setId(bodypk);
        body.setSeq(seq);
        body.setKcpersonid(kcpersonid);
        body.setGubun(gubun);
        body.setIndate(indate);

        // 데이터 insert
        try {
            e063Repository.save(head);
            e064Repository.save(body);

            result.success = true;
            result.message = "저장을 성공했습니다.";
        }catch (Exception e) {
            result.success = false;
            result.message = "저장 실패(" + e.getMessage() + ")";
        }
        return result;
    }
    // 064 테이블 no 컬럼 Max값 +1
    public String getNextNoForKey(String spjangcd, Integer personid, String papercd) {
        // 현재 max(no) 조회
        String maxNo = e064Repository.findMaxNo(spjangcd, personid, papercd);
        int next = maxNo != null ? Integer.parseInt(maxNo) + 1 : 1;
        return String.valueOf(next); // 예: 001, 002
    }

}

