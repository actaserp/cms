package mes.app.request;

import lombok.extern.slf4j.Slf4j;
import mes.app.request.service.RequestService;
import mes.domain.entity.TbAs010;
import mes.domain.entity.TbAs011;
import mes.domain.entity.User;
import mes.domain.entity.commute.TB_PB201;
import mes.domain.model.AjaxResult;
import mes.domain.repository.TbAs010Repository;
import mes.domain.repository.TbAs011Repository;
import mes.domain.repository.TbAs020Repository;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.FileSystemResource;
import org.springframework.core.io.Resource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;

import javax.servlet.http.HttpServletRequest;
import java.io.File;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.sql.Timestamp;
import java.util.*;

@Slf4j
@RestController
@RequestMapping("/api/request")
public class RequestController {

    @Autowired
    RequestService requestService;

    @Autowired
    private TbAs011Repository tbAs011Repository;

    @Autowired
    private TbAs010Repository tbAs010Repository;

    @Autowired
    private TbAs020Repository tbAs020Repository;

    // 거래처 정보 조회
    @GetMapping("/searchUser")
    public AjaxResult getUserInfo(
            HttpServletRequest request,
            @RequestParam(value="compid") String compid,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        Map<String, Object> searchData  = requestService.searchUserInfo( compid );

        result.data = searchData;

        return result;
    }

    // 거래처 정보 조회
    @GetMapping("/userInfo")
    public AjaxResult userInfo(
            HttpServletRequest request,
            @RequestParam(value="userId") String userid,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        Map<String, Object> searchData  = requestService.boolUserInfo( userid );

        result.data = searchData;

        return result;
    }

    // 요청사항 조회
    @GetMapping("/search")
    public AjaxResult searchDatas(
            HttpServletRequest request,
            @RequestParam(value="searchfrdate") String searchfrdate,
            @RequestParam(value="searchtodate") String searchtodate,
            @RequestParam(value="searchCompCd", required=false) String searchCompCd,
            @RequestParam(value="reqType", required=false) String reqType,
            @RequestParam(value="recyn", required=false) String recyn,
            @RequestParam(value="reqPer", required=false) String usernm,
            @RequestParam(value="spjangcd", required=false) String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        List<Map<String, Object>> searchDatas  = requestService.searchDatas(
                searchfrdate
                , searchtodate
                , searchCompCd
                , reqType
                , recyn
                , usernm
                , spjangcd
        );

        result.data = searchDatas;

        return result;
    }

    // 상세정보 조회
    @GetMapping("/detail")
    public AjaxResult getRequestDetail(
            @RequestParam("id") Integer id,
            HttpServletRequest request) {
        AjaxResult result = new AjaxResult();
        
        Map<String, Object> item = requestService.getDetail(id);
        result.data = item;
        
        return result;
    }

    // 저장
    @PostMapping("/save")
    @Transactional
    public AjaxResult saveRequest(@RequestBody Map<String, Object> payload, Authentication auth) {
        User user = (User) auth.getPrincipal();
        AjaxResult result = new AjaxResult();

        try {
            Integer id = payload.get("id") != null && !payload.get("id").toString().isEmpty()
                    ? Integer.parseInt(payload.get("id").toString())
                    : null;

            // ✅ 새 파일명 (클라이언트에서 업로드된 파일명)
            String newFileName = payload.get("as_file") != null
                    ? payload.get("as_file").toString()
                    : null;

            // ✅ 기존 데이터 확인
            if (id != null) {
                Optional<TbAs010> existingOpt = tbAs010Repository.findById(id);
                if (existingOpt.isPresent()) {
                    TbAs010 existing = existingOpt.get();

                    // ✅ 접수 이후 수정 불가 로직
                    if (existing.getRecdate() != null) {
                        result.success = false;
                        result.message = "접수 이후 수정, 삭제 처리가 불가능합니다.";
                        return result;
                    }

                    // ✅ 기존 파일 삭제 로직
                    String oldFileName = existing.getAsFile();
                    if (oldFileName != null && !oldFileName.isEmpty()
                            && newFileName != null && !newFileName.equals(oldFileName)) {
                        File oldFile = new File("C:/temp/as_request/files/" + oldFileName);
                        if (oldFile.exists()) {
                            boolean deleted = oldFile.delete();
                            if (deleted) {
                                log.info("🗑 기존 파일 삭제 완료: {}", oldFile.getAbsolutePath());
                            } else {
                                log.warn("⚠ 기존 파일 삭제 실패: {}", oldFile.getAbsolutePath());
                            }
                        }
                    }
                }
            }

            // ✅ 정상 저장 로직
            result = requestService.saveRequest(payload, user);
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = e.getMessage();
        }

        return result;
    }


    // 요청사항 삭제 (첨부파일 포함 정리)
    @PostMapping("/delete")
    @Transactional
    public AjaxResult deleteRequest(@RequestParam Map<String, Object> params) {
        AjaxResult result = new AjaxResult();
        try {
            Integer id = Integer.parseInt(params.get("id").toString());

            TbAs010 entity = tbAs010Repository.findById(id)
                    .orElseThrow(() -> new RuntimeException("데이터를 찾을 수 없습니다."));

            // ✅ recdate가 존재하면 삭제 불가
            if (entity.getRecdate() != null) {
                result.success = false;
                result.message = "접수이후 수정,삭제 처리가 불가능합니다.";
                return result;
            }

            // ✅ 첨부파일 삭제
            String asFile = entity.getAsFile();
            if (asFile != null && !asFile.isEmpty()) {
                File file = new File("C:/temp/as_request/files/" + asFile);
                if (file.exists()) file.delete();
            }

            tbAs010Repository.delete(entity);
            result.success = true;
        } catch (Exception e) {
            e.printStackTrace();
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }



    @PostMapping("/uploadFile")
    public AjaxResult uploadFile(@RequestParam("uploadFile") MultipartFile file) {
        AjaxResult result = new AjaxResult();

        try {
            if (file.isEmpty()) {
                result.success = false;
                result.message = "파일이 비어 있습니다.";
                return result;
            }

            // ✅ 원본 파일명 및 확장자 추출
            String originalName = file.getOriginalFilename();
            String ext = FilenameUtils.getExtension(originalName);
            String uuid = UUID.randomUUID().toString();

            // ✅ 저장 경로 생성 (없으면 자동 생성)
            File dir = new File("C:/temp/as_request/files");
            if (!dir.exists()) {
                boolean created = dir.mkdirs();
                if (!created) {
                    throw new RuntimeException("파일 저장 경로 생성 실패: " + dir.getAbsolutePath());
                }
            }

            // ✅ 실제 저장 파일명 (UUID + 원본 확장자 유지)
            String newFileName = uuid + (ext != null && !ext.isEmpty() ? "." + ext : "");
            File dest = new File(dir, newFileName);

            // ✅ 파일 저장
            file.transferTo(dest);

            log.info("📂 파일 업로드 완료: {}", dest.getAbsolutePath());

            result.success = true;
            result.data = newFileName;   // 업로드된 파일명 반환
            result.message = "파일 업로드 성공";
        }
        catch (SecurityException se) {
            log.error("🚫 파일 권한 오류", se);
            result.success = false;
            result.message = "서버에 파일을 쓸 권한이 없습니다: " + se.getMessage();
        }
        catch (Exception e) {
            log.error("❌ 파일 업로드 실패", e);
            result.success = false;
            result.message = "파일 업로드 실패: " + e.getMessage();
        }

        return result;
    }

    @GetMapping("/downFile")
    public ResponseEntity<Resource> downFile(@RequestParam("fileName") String fileName) {
        try {
            // ✅ 실제 파일 경로 (업로드 경로와 동일)
            File file = new File("C:/temp/as_request/files/" + fileName);

            if (!file.exists()) {
                log.warn("❌ 파일 없음: {}", file.getAbsolutePath());
                return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
            }

            // ✅ 리소스 래핑
            Resource resource = new FileSystemResource(file);

            // ✅ 확장자 추출 (원본 파일의 확장자 유지)
            String ext = "";
            int dotIndex = file.getName().lastIndexOf(".");
            if (dotIndex > 0) {
                ext = file.getName().substring(dotIndex); // 예: .png, .pdf 등
            }

            // ✅ 다운로드 시 표시될 파일명 (고정)
            String downloadName = "유지보수_요청_첨부파일" + ext;

            // ✅ 파일명 인코딩 (한글 깨짐 방지)
            String encodedName = URLEncoder.encode(downloadName, StandardCharsets.UTF_8)
                    .replaceAll("\\+", "%20");

            // ✅ Content-Type 자동 탐지
            String contentType = Files.probeContentType(file.toPath());
            if (contentType == null) contentType = "application/octet-stream";

            // ✅ 다운로드 응답
            return ResponseEntity.ok()
                    .contentType(MediaType.parseMediaType(contentType))
                    .header(HttpHeaders.CONTENT_DISPOSITION,
                            "attachment; filename*=UTF-8''" + encodedName)
                    .header(HttpHeaders.CONTENT_LENGTH, String.valueOf(file.length()))
                    .body(resource);

        } catch (Exception e) {
            log.error("❌ 파일 다운로드 실패", e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).build();
        }
    }

    // 거래처 검색 팝업조회
    @GetMapping("/getComp")
    public AjaxResult getComp(
            HttpServletRequest request,
            @RequestParam(value="searchCode") String searchCode,
            @RequestParam(value="searchName") String searchName,
            @RequestParam(value="spjangcd", required=false) String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        List<Map<String, Object>> searchDatas  = requestService.getComp(
                searchCode
                , searchName
                , spjangcd
        );

        result.data = searchDatas;

        return result;
    }

    // 사용자(본사담당) 검색 팝업조회
    @GetMapping("/getUser")
    public AjaxResult getUser(
            HttpServletRequest request,
            @RequestParam(value="searchCode") String searchCode,
            @RequestParam(value="searchName") String searchName,
            @RequestParam(value="spjangcd", required=false) String spjangcd,
            Authentication auth) {
        AjaxResult result = new AjaxResult();
        User user = (User)auth.getPrincipal();
        String username = user.getUsername();

        List<Map<String, Object>> searchDatas  = requestService.getUser(
                searchCode
                , searchName
                , spjangcd
        );

        result.data = searchDatas;

        return result;
    }

}
