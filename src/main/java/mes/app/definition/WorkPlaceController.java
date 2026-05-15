package mes.app.definition;

import com.fasterxml.jackson.databind.JsonNode;
import mes.app.cms.service.CmsTokenService;
import mes.app.definition.service.WorkPlaceService;
import mes.domain.entity.Tb_xa012;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_xa012Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workplace")
public class WorkPlaceController {

    @Autowired WorkPlaceService workPlaceService;
    @Autowired Tb_xa012Repository tbXa012Repository;
    @Autowired CmsTokenService cmsTokenService;

    private boolean isAdmin(Authentication auth) {
        User user = (User) auth.getPrincipal();
        return "admin".equals(user.getUsername()) || Boolean.TRUE.equals(user.getSuperUser());
    }

    // ── 사업장 기본 CRUD ──────────────────────────────────

    @GetMapping("/read")
    public AjaxResult getSpjangInfo(Authentication auth) {
        AjaxResult result = new AjaxResult();
        if (!isAdmin(auth)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        List<Tb_xa012> spjangs = tbXa012Repository.findAll(Sort.by(Sort.Direction.DESC, "spjangcd"));
        List<Map<String, Object>> data = spjangs.stream()
                .map(s -> workPlaceService.getSpjangWithCmsErp(s.getSpjangcd()))
                .collect(java.util.stream.Collectors.toList());
        result.data = data;
        return result;
    }

    @GetMapping("/my")
    public AjaxResult getMySpjangInfo(Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        result.data = workPlaceService.getSpjangWithCmsErp(user.getSpjangcd());
        return result;
    }

    @PostMapping("/save")
    public AjaxResult saveSpjangInfo(
            @RequestBody Map<String, Object> req,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        String spjangcd = (String) req.get("spjangcd");

        if (!isAdmin(auth) && !user.getSpjangcd().equals(spjangcd)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        try {
            workPlaceService.saveSpjangWithCmsErp(req);
            result.success = true;
            result.message = "저장되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "저장 중 오류 발생: " + e.getMessage();
        }
        return result;
    }

    @PostMapping("/delete")
    public AjaxResult deleteSpjangInfo(
            @RequestBody Map<String, Object> param,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        if (!isAdmin(auth)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        try {
            tbXa012Repository.deleteById((String) param.get("spjangcd"));
            result.success = true;
            result.message = "삭제되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "삭제 중 오류 발생";
        }
        return result;
    }

    // ── CMS ───────────────────────────────────────────────

    @GetMapping("/cms/{spjangcd}")
    public AjaxResult getCms(
            @PathVariable String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        if (!isAdmin(auth) && !user.getSpjangcd().equals(spjangcd)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        result.data = workPlaceService.getCms(spjangcd);
        return result;
    }

    // ── 금결원 이용기관 상세 조회 ─────────────────────────

    @GetMapping("/cms/institute-detail/{spjangcd}")
    public AjaxResult getInstituteDetail(
            @PathVariable String spjangcd,
            @RequestParam(required = false) String cmsCode,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        if (!isAdmin(auth) && !user.getSpjangcd().equals(spjangcd)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        try {
            JsonNode data = cmsTokenService.getInstituteDetailByCode(cmsCode);
            result.data = data;
            result.success = true;
        } catch (Exception e) {
            result.success = false;
            result.message = "금결원 조회 실패: " + e.getMessage();
        }
        return result;
    }

    // ── ERP 연결 테스트 ───────────────────────────────────

    @PostMapping("/erp-test")
    public AjaxResult erpTest(
            @RequestBody Map<String, Object> req,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        try {
            workPlaceService.testErpConnection(req, user.getSpjangcd());
            result.success = true;
            result.message = "연결 성공";
        } catch (Exception e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    // ── 분점 ──────────────────────────────────────────────

    @GetMapping("/branches/{spjangcd}")
    public AjaxResult getBranches(
            @PathVariable String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        if (!isAdmin(auth) && !user.getSpjangcd().equals(spjangcd)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        result.data = workPlaceService.getBranches(spjangcd);
        return result;
    }

    @PostMapping("/branch/save")
    public AjaxResult saveBranch(
            @RequestBody Map<String, Object> req,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        if (!isAdmin(auth) && !user.getSpjangcd().equals(req.get("parentSpjangcd"))) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        try {
            workPlaceService.saveBranch(req);
            result.success = true;
            result.message = "저장되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "저장 중 오류 발생: " + e.getMessage();
        }
        return result;
    }

    @DeleteMapping("/branch/{spjangcd}")
    public AjaxResult deleteBranch(
            @PathVariable String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        try {
            workPlaceService.deleteBranch(spjangcd);
            result.success = true;
            result.message = "삭제되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "삭제 중 오류 발생: " + e.getMessage();
        }
        return result;
    }

    // ── 세무서 팝업 ───────────────────────────────────────

    @GetMapping("/readPopup")
    public AjaxResult readPopup(
            @RequestParam String spjangcd,
            @RequestParam String taxcd,
            @RequestParam String taxnm2,
            @RequestParam String taxjiyuk,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = workPlaceService.getPopupList(taxcd, taxnm2, taxjiyuk);
            result.success = true;
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "팝업조회 중 오류 발생";
        }
        return result;
    }

    @GetMapping("/banks")
    public AjaxResult getBanks() {
        AjaxResult result = new AjaxResult();
        result.data = workPlaceService.getBankList();
        return result;
    }

    @GetMapping("/detail/{spjangcd}")
    public AjaxResult getSpjangDetail(
            @PathVariable String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        if (!isAdmin(auth) && !user.getSpjangcd().equals(spjangcd)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        result.data = workPlaceService.getSpjangWithCmsErp(spjangcd);
        return result;
    }

    @GetMapping("/cms/institute-detail")
    public AjaxResult getInstituteDetailByCode(
            @RequestParam String cmsCode,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = cmsTokenService.getInstituteDetailByCode(cmsCode);
            result.success = true;
        } catch (Exception e) {
            result.success = false;
            result.message = "금결원 조회 실패: " + e.getMessage();
        }
        return result;
    }
}