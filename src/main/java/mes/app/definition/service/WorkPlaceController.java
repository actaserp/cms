package mes.app.definition.service;

import mes.app.mobile.Service.AttendanceCurrentService;
import mes.domain.entity.Tb_xa012;
import mes.domain.entity.User;
import mes.domain.model.AjaxResult;
import mes.domain.repository.Tb_xa012Repository;
import mes.domain.repository.mobile.TB_PB204Repository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.domain.Sort;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/workplace")
public class WorkPlaceController {
    @Autowired
    WorkPlaceService workPlaceService;
    @Autowired
    Tb_xa012Repository tbXa012Repository;
    
    private boolean isAdmin(Authentication auth) {
        User user = (User) auth.getPrincipal();
        return "admin".equals(user.getUsername()) || Boolean.TRUE.equals(user.getSuperUser());
    }

    // 사업장정보 리스트 조회 (어드민 전용)
    @GetMapping("/read")
    public AjaxResult getSpjangInfo(
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        if (!isAdmin(auth)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        result.data = tbXa012Repository.findAll(Sort.by(Sort.Direction.DESC, "spjangcd"));
        return result;
    }

    // 현재 사용자의 사업장 조회
    @GetMapping("/my")
    public AjaxResult getMySpjangInfo(
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();
        tbXa012Repository.findById(user.getSpjangcd()).ifPresent(item -> result.data = item);
        return result;
    }

    // 사업장 저장 (어드민: 모든 사업장, 일반: 자기 사업장만)
    @PostMapping("/save")
    public AjaxResult saveSpjangInfo(
            @ModelAttribute Tb_xa012 tbXa012,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User) auth.getPrincipal();

        // 어드민이 아니면 자기 spjangcd 외 저장 불가
        if (!isAdmin(auth) && !user.getSpjangcd().equals(tbXa012.getSpjangcd())) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }

        try {
            tbXa012Repository.save(tbXa012);
            result.success = true;
            result.message = "저장되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "저장 중 오류 발생";
        }
        return result;
    }

    // 사업장 삭제 (어드민 전용)
    @PostMapping("/delete")
    public AjaxResult deleteSpjangInfo(
            @RequestBody Map<String, Object> param,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        if (!isAdmin(auth)) {
            result.success = false;
            result.message = "권한이 없습니다.";
            return result;
        }
        String spjangcd = (String) param.get("spjangcd");
        try {
            tbXa012Repository.deleteById(spjangcd);
            result.success = true;
            result.message = "삭제되었습니다.";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "삭제 중 오류 발생";
        }
        return result;
    }
    // 세무서 팝업 리스트 조회
    @GetMapping("/readPopup")
    public AjaxResult readPopup(
            @RequestParam String spjangcd,
            @RequestParam String taxcd,
            @RequestParam String taxnm2,
            @RequestParam String taxjiyuk,
            HttpServletRequest request,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        try {
            result.data = workPlaceService.getPopupList(taxcd, taxnm2, taxjiyuk);
            result.success = true;
            result.message = "팝업데이터 조회 성공";
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = "팝업조회 중 오류 발생";
        }
        return result;
    }
}
