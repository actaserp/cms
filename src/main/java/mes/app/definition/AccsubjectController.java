package mes.app.definition;


import mes.app.definition.service.AccSubjectService;
import mes.domain.entity.*;
import mes.domain.model.AjaxResult;
import mes.domain.repository.AccSubjectRepository;
import mes.domain.repository.AccmanageRepository;
import mes.domain.services.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.security.core.Authentication;
import org.springframework.util.MultiValueMap;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.sql.Timestamp;
import java.util.*;
import java.util.stream.Collectors;

@RestController
@RequestMapping("/api/definition/Accsubject")
public class AccsubjectController {

    @Autowired
    AccSubjectRepository accSubjectRepository;

    @Autowired
    AccSubjectService accSubjectService;

    @Autowired
    AccmanageRepository accmanageRepository;

    @GetMapping("/read")
    public AjaxResult getAccList(@RequestParam(value ="spjangcd") String spjangcd,
                                 @RequestParam(value ="acccd") String acccd,
                                 @RequestParam(value ="accnm") String accnm,
                                 @RequestParam(value ="useyn") String useyn
                                 ) {
        AjaxResult result = new AjaxResult();
        result.data = this.accSubjectService.getAccList(spjangcd, acccd, accnm, useyn);
        return result;
    }

    @PostMapping("/save")
    public AjaxResult saveAcc(
            @RequestParam(value="acccd") String acccd, // 계정코드
            @RequestParam(value="uacccd" , required=false) String uacccd, // 상위계정
            @RequestParam(value="drcr") String drcr, //대손
            @RequestParam(value="accnm") String accnm, //계정명
            @RequestParam(value="dcpl") String dcpl, //대손
            @RequestParam(value="spyn" , required=false) String spyn, // 전표사용
            @RequestParam(value="accprtnm") String accprtnm, // 양식명 
            @RequestParam(value="etccode" , required=false) String etccode, // 연결코드 accnm
            @RequestParam(value="cacccd" , required=false) String cacccd, // 차감계정 acccd
            @RequestParam(value="acclv", required=false) String acclvStr,
            @RequestParam(value="useyn", required=false) String useyn,
            @RequestParam(value ="spjangcd") String spjangcd,
            Authentication auth
    ) {

        User user = (User)auth.getPrincipal();
        AjaxResult result = new AjaxResult();
        Accsubject acc = new Accsubject();

        int lv = 0;
        if (acclvStr == null || acclvStr.trim().isEmpty()) {
            acc.setAcclv(lv);
        } else {
            try {
                acc.setAcclv(Integer.parseInt(acclvStr) + 1);
            } catch (NumberFormatException e) {
                acc.setAcclv(lv); // 또는 예외 처리
            }
        }


        acc.setUseyn(useyn != null ? useyn : "N");
        acc.setSpyn(spyn != null ? spyn : "0");

        acc.setAcccd(acccd);
        acc.setUacccd(uacccd); //상위계정
        acc.setDrcr(drcr);  // 차대
        acc.setAccnm(accnm); // 계정명
        acc.setDcpl(dcpl); // 대손
        acc.setAccprtnm(accprtnm); // 양식명
        acc.setEtccode(etccode); // 연결코드
        acc.setCacccd(cacccd); //차감계정
        acc.setSpjangcd(spjangcd);

        this.accSubjectRepository.save(acc);
        result.data = acc;

        return result;

    }

    @PostMapping("/delete")
    public AjaxResult deleteAcc(@RequestParam(value="id") String id ) {
        // accSubject 삭제
        this.accSubjectRepository.deleteById(id);

        // accManage 삭제
        List<Accmanage> accmanageList = accmanageRepository.findById_Acccd(id);
        if (!accmanageList.isEmpty()) {
            accmanageRepository.deleteAll(accmanageList);
        }

        AjaxResult result = new AjaxResult();
        return result;
    }

    @RequestMapping("/detail")
    public AjaxResult getAccDetail(
            @RequestParam(value="id") String id
    ) {
        AjaxResult result = new AjaxResult();
        result.data = this.accSubjectService.getAccDetail(id);
        return result;
    }



    @PostMapping("/add")
    @Transactional
    public AjaxResult saveTestMaster(
            @RequestParam("Q") String qJson,
            @RequestParam("id") String id,
            @RequestParam(value = "deleteYn", required = false, defaultValue = "N") String deleteYn,
            HttpServletRequest request,
            Authentication auth) {

        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        List<Accmanage> savedData = new ArrayList<>();

        List<Map<String, Object>> data = CommonUtil.loadJsonListMap(qJson);


        for (Map<String, Object> item : data) {
            String itemcd = (String) item.get("code");
            String itemnm = (String) item.get("name");
            Boolean required = (Boolean) item.get("required");
            Boolean used = (Boolean) item.get("used");

            AccmanageId accmanageId = new AccmanageId();
            accmanageId.setAcccd(id);
            accmanageId.setItemcd(itemcd);

            Optional<Accmanage> existingData = accmanageRepository.findById(accmanageId);
          /*  System.out.println("existingData확인: " + existingData);*/

            Accmanage target;
            if (existingData.isPresent()) {
                target = existingData.get();
            } else {
                target = new Accmanage();
                target.setId(accmanageId);
            }

            target.setItemnm(itemnm);
            target.setEssyn(required != null && required ? "1" : "0");
            target.setUseyn(used != null && used ? "Y" : "N");

            Accmanage saved = accmanageRepository.save(target);

            savedData.add(saved);
        }

        // deleteYn이 "Y"일 때만 삭제 실행
        if ("Y".equalsIgnoreCase(deleteYn)) {
            // 클라이언트에서 넘어온 데이터의 "code" 값 (삭제할 항목) 추출
            Set<String> deletedItemcds = data.stream()
                    .map(item -> (String) item.get("code"))
                    .collect(Collectors.toSet());

            // DB에서 동일한 acccd와 itemcd를 기준으로 삭제할 항목을 조회
            List<Accmanage> existingRecords = accmanageRepository.findById_Acccd(id); // `acccd`로 모든 항목 조회
            for (Accmanage existing : existingRecords) {
                // 클라이언트에서 넘어온 항목 중 삭제할 항목에 해당하는 경우 삭제
                if (deletedItemcds.contains(existing.getId().getItemcd())) {
                    accmanageRepository.delete(existing);
                 /*   System.out.println("삭제된 데이터: " + existing);*/
                }
            }
        }

        for (Accmanage accmanage : savedData) {
            /*System.out.println("저장된 데이터 확인: " + accmanage);*/
        }

        result.data = savedData;
        return result;
    }



    @RequestMapping("/AddDetail")
    public AjaxResult getAddDetail(
            @RequestParam(value="id") String id
    ) {
        AjaxResult result = new AjaxResult();
        result.data = this.accSubjectService.getAddDetail(id);
        return result;
    }


    /*
* 계정과목관리 차감계정 검색
* */
    @GetMapping("/search_acc")
    public AjaxResult getAccSearchList(
            @RequestParam(value="searchCode", required=false) String code,
            @RequestParam(value="searchName", required=false) String name,
            @RequestParam(value ="spjangcd") String spjangcd
    ) {

        AjaxResult result = new AjaxResult();
        result.data = this.accSubjectService.getAccSearchitem(code,name,spjangcd);
        return result;
    }


    @GetMapping("/list")
    public List<Map<String, String>> getAcccdList() {
        return accSubjectService.getAccCodeAndAccnmAndAcclvList();
    }


}
