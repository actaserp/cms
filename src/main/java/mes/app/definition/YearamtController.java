package mes.app.definition;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import mes.app.definition.service.YearamtService;
import mes.domain.entity.Area;
import mes.domain.entity.User;
import mes.domain.entity.Yearamt;
import mes.domain.entity.YearamtId;
import mes.domain.model.AjaxResult;
import mes.domain.repository.YearamtRepository;
import mes.domain.services.CommonUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.transaction.Transactional;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/definition/yearamt")
public class YearamtController {

    @Autowired
    YearamtService yearamtService;

    @Autowired
    YearamtRepository yearamtRepository;

    @Autowired
    ObjectMapper objectMapper;

    @GetMapping("/read")
    public AjaxResult getYearamtList(
        @RequestParam(value = "cboYear", required = false) String year,
        @RequestParam(value = "ioflag", required = false) String ioflag,
        @RequestParam(value = "searchId", required = false) String cltid,
        @RequestParam(value = "searchname", required = false) String name,
        @RequestParam(value = "endyn", required = false) String endyn,
        @RequestParam(value = "spjangcd") String spjangcd

    ) {
        AjaxResult result = new AjaxResult();
        //log.info("year:{}, ioflag:{}, cltid:{}, name:{},endyn:{}, spjangcd:{}", year, ioflag, cltid, name,endyn, spjangcd);
        result.data = this.yearamtService.getYearamtList(year, ioflag, cltid, name,endyn, spjangcd);
        return result;
    }

    @PostMapping("/magam")
    @Transactional
    public AjaxResult magam(@RequestBody Map<String, Object> req) {
        //log.info("[YEARAMT][MAGAM] 요청 도착");

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) req.get("list");

        AjaxResult result = new AjaxResult();
        if (dataList == null || dataList.isEmpty()) {
            log.warn("[YEARAMT][MAGAM] list가 비어있음");
            result.success = false;
            result.message = "요청 데이터(list)가 비어 있습니다.";
            return result;
        }

        // 요청 요약 로그 (첫 1건 + 총 개수)
        try {
            String firstJson = objectMapper.writerWithDefaultPrettyPrinter()
                .writeValueAsString(dataList.get(0));
            //log.info("[YEARAMT][MAGAM] 총 {}건, 첫 아이템=\n{}", dataList.size(), firstJson);
        } catch (Exception e) {
            log.warn("[YEARAMT][MAGAM] 첫 아이템 직렬화 실패: {}", e.getMessage());
        }

        // 같은 키가 여러 번 오면 마지막 항목으로 덮어쓰기
        Map<YearamtId, Yearamt> upserts = new LinkedHashMap<>();

        for (int i = 0; i < dataList.size(); i++) {
            Map<String, Object> item = dataList.get(i);

            String ioflag = CommonUtil.tryString(item.get("ioflag"));   // "0" or "1"
            String yyyymm = CommonUtil.tryString(item.get("yyyymm"));   // "YYYYMM"
            Integer cltcd = CommonUtil.tryIntNull(item.get("cltcd"));
            Integer yearamt = CommonUtil.tryIntNull(item.get("yearamt"));
            String endynReq = CommonUtil.tryString(item.get("endyn"));
            String spjangcdReq = CommonUtil.tryString(item.get("spjangcd"));


            // 기본 유효성
            if (ioflag == null || ioflag.isEmpty() || yyyymm == null || yyyymm.length() != 6 || cltcd == null) {
                //log.warn("[YEARAMT][MAGAM][{}] 스킵(유효성 실패): ioflag={}, yyyymm={}, cltcd={}", i, ioflag, yyyymm, cltcd);
                continue;
            }

            // 키 생성
            YearamtId id = new YearamtId(ioflag, yyyymm, cltcd);

            // 기존 값 조회(업데이트) 또는 신규 생성(인서트)
            Yearamt y = upserts.get(id);
            if (y == null) {
                y = yearamtRepository.findById(id).orElseGet(() -> {
                    Yearamt t = new Yearamt();
                    t.setId(id);
                    return t;
                });
            }

            // 값 매핑
            y.setYearamt(yearamt != null ? yearamt : 0);
            y.setEndyn("Y");

            String spjangcd = spjangcdReq;
            if (spjangcd != null && spjangcd.length() > 2) spjangcd = spjangcd.substring(0, 2);
            y.setSpjangcd(spjangcd);

            upserts.put(id, y);

            /*log.info("[YEARAMT][MAGAM][{}] upsert 준비: id=[ioflag={}, yyyymm={}, cltcd={}], yearamt={}, endyn={}, spjangcd={}",
                i, id.getIoflag(), id.getYyyymm(), id.getCltcd(), y.getYearamt(), y.getEndyn(), y.getSpjangcd());*/
        }

        if (upserts.isEmpty()) {
            log.warn("[YEARAMT][MAGAM] 유효 항목 0건(저장 안 함)");
            result.success = false;
            result.message = "유효한 항목이 없습니다.";
            return result;
        }

        try {
            yearamtRepository.saveAll(upserts.values());
            //log.info("[YEARAMT][MAGAM] saveAll 완료 ({}건)", upserts.size());
        } catch (DataIntegrityViolationException e) {
            String root = (e.getMostSpecificCause() != null) ? e.getMostSpecificCause().getMessage() : e.getMessage();
            log.error("[YEARAMT][MAGAM] 무결성 오류: {}", root, e);

            AjaxResult err = new AjaxResult();
            err.success = false;
            err.message = "데이터 무결성 오류: " + root;
            return err;

        } catch (Exception e) {
            log.error("[YEARAMT][MAGAM] 서버 오류: {}", e.getMessage(), e);

            AjaxResult err = new AjaxResult();
            err.success = false;
            err.message = "서버 오류: " + e.getMessage();
            return err;
        }

        result.success = true;
        result.message = "마감 처리 완료";
        return result;
    }

    @PostMapping("/magamCancel")
    @Transactional
    public AjaxResult deleteYearamtMagamCancel(
        @RequestBody Map<String, Object> body,
        HttpServletRequest request,
        Authentication auth) {

        //log.info("[YEARAMT][MAGAM_CANCEL] 요청 수신");

        AjaxResult result = new AjaxResult();

        Object rawList = body.get("list");
        if (!(rawList instanceof List<?> raw)) {
            // log.warn("[YEARAMT][MAGAM_CANCEL] list 비어있음/형식 오류");
            result.success = false;
            result.message = "삭제할 데이터가 없습니다.";
            return result;
        }

        @SuppressWarnings("unchecked")
        List<Map<String, Object>> dataList = (List<Map<String, Object>>) raw;

        String defaultSpjangcd = String.valueOf(body.getOrDefault("spjangcd", "ZZ"));

        for (int i = 0; i < dataList.size(); i++) {
            Map<String, Object> item = dataList.get(i);

            Object cltcdObj = item.getOrDefault("cltcd", item.get("id"));
            Integer cltcd = null;
            if (cltcdObj instanceof Number) cltcd = ((Number) cltcdObj).intValue();
            else if (cltcdObj != null) { try { cltcd = Integer.valueOf(String.valueOf(cltcdObj)); } catch (Exception ignore) {} }

            String ioflagRaw = String.valueOf(item.get("ioflag"));
            String ioflag = "1".equals(ioflagRaw) ? "1" : "0";

            String yyyymm = String.valueOf(item.get("yyyymm"));
            yyyymm = yyyymm.replaceAll("[^0-9]", "");
            if (yyyymm.length() != 6) {
                log.warn("[YEARAMT][MAGAM_CANCEL][{}] 스킵(yyyymm 형식 오류): {}", i, yyyymm);
                continue;
            }

            String spjangcd = String.valueOf(item.getOrDefault("spjangcd", defaultSpjangcd));

           /* log.info("[YEARAMT][MAGAM_CANCEL][{}] recv cltcd={}, ioflag={}, yyyymm={}, spjangcd={}",
                i, cltcd, ioflag, yyyymm, spjangcd);*/

            if (cltcd == null || ioflag.isBlank()) {
                //log.warn("[YEARAMT][MAGAM_CANCEL][{}] 스킵(유효성 실패) cltcd={}, ioflag={}", i, cltcd, ioflag);
                continue;
            }

            try {
                yearamtRepository.deleteByIdIoflagAndIdYyyymmAndIdCltcdAndSpjangcd(ioflag, yyyymm, cltcd, spjangcd);
                // log.info("[YEARAMT][MAGAM_CANCEL][{}] 삭제 수행 완료", i);
            } catch (Exception e) {
                log.error("[YEARAMT][MAGAM_CANCEL][{}] 삭제 중 오류: {}", i, e.getMessage(), e);
            }
        }

        result.success = true;
        result.message = "마감이 취소되었습니다.";
        return result;
    }

}