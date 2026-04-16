package mes.app.transaction;

import lombok.extern.slf4j.Slf4j;
import mes.app.transaction.service.ProjectRegistrationServicr;
import mes.domain.entity.TB_DA003;
import mes.domain.entity.TB_DA003Id;
import mes.domain.model.AjaxResult;
import mes.domain.repository.ProjectRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.util.List;
import java.util.Map;
import java.util.Optional;

@Slf4j
@RestController
@RequestMapping("/api/transaction/projectRegistration")
public class ProjectRegistrationController {  //프로젝트 관리

  @Autowired
  ProjectRegistrationServicr projectRegistrationServicr;

  @Autowired
  ProjectRepository projectRepository;

  @GetMapping("/read")
  public AjaxResult getProjectList(@RequestParam(value = "srchStartDt") String srchStartDt,
                                   @RequestParam(value = "srchEndDt") String srchEndDt,
                                   @RequestParam(value ="spjangcd") String spjangcd,
                                   @RequestParam(value = "cboCompany") String cboCompany,
                                   @RequestParam(value = "txtDescription") String txtDescription,
                                   HttpServletRequest request) {
    //log.info("프로젝트 read -srchStartDt:{}, srchEndDt:{},spjangcd:{},:cboCompany:{}, txtDescription: {}", srchStartDt, srchEndDt, spjangcd, cboCompany, txtDescription);

    srchStartDt = formatDate8(srchStartDt);
    srchEndDt = formatDate8(srchEndDt);
    List<Map<String, Object>> items = this.projectRegistrationServicr.getProjectList(srchStartDt, srchEndDt, spjangcd, cboCompany,txtDescription);

    AjaxResult result = new AjaxResult();
    result.data = items;

    return result;
  }

  //저장
  @PostMapping("/save")
  public AjaxResult ProjectListSave(@RequestParam(value ="projno" , required = false) String projno,
                                    @RequestParam(value ="projnm") String projnm,
                                    @RequestParam(value ="balcltnm") String balcltnm,
                                    @RequestParam(value ="balcltcd") Integer balcltcd,
                                    @RequestParam(value ="stdate") String stdate,
                                    @RequestParam(value ="eddate") String eddate,
                                    @RequestParam(value ="contdate") String contdate,
                                    @RequestParam(value ="remark")String remark,
                                    @RequestParam(value ="spjangcd") String spjangcd){
    /*log.info("프로젝트관리 projno:{}, projnm:{}, balcltnm:{}, balcltcd:{}, stdate:{},eddate:{}, contdate:{}, remark:{},spjangcd:{}",
        projno, projnm, balcltnm, balcltcd, stdate, eddate, contdate, remark,spjangcd);*/

    AjaxResult result = new AjaxResult();

    stdate = formatDate8(stdate);
    eddate = formatDate8(eddate);
    contdate = formatDate8(contdate);
    // 1. 신규 등록
    if (projno == null || projno.trim().isEmpty()) {
      String newProjNo = generateNewProjectNo(); // 별도 메서드 필요
      TB_DA003 newProject = new TB_DA003();

      newProject.setProjnm(projnm);
      newProject.setBalcltnm(balcltnm);
      newProject.setBalcltcd(balcltcd);
      newProject.setStdate(stdate);
      newProject.setEddate(eddate);
      newProject.setContdate(contdate);
      newProject.setRemark(remark);
      newProject.setId(new TB_DA003Id(spjangcd, newProjNo));

      this.projectRepository.save(newProject);
      result.success=true;
      result.message="신규 저장 완료";
      result.data=newProjNo;
    }
    // 2. 기존 데이터 수정
    else {
      Optional<TB_DA003> optional = this.projectRepository.findById(new TB_DA003Id(spjangcd, projno));
      if (optional.isPresent()) {
        TB_DA003 existing = optional.get();
        existing.setProjnm(projnm);
        existing.setBalcltnm(balcltnm);
        existing.setBalcltcd(balcltcd);
        existing.setStdate(stdate);
        existing.setEddate(eddate);
        existing.setContdate(contdate);
        existing.setRemark(remark);

        this.projectRepository.save(existing);
        result.success=(true);
        result.message=("수정 완료");
      } else {
        result.success = (false);
        result.message = ("수정 실패: 해당 프로젝트 없음");
      }
    }

    return result;
  }
  private String formatDate8(String dateStr) {
    return dateStr != null ? dateStr.replace("-", "") : null;
  }

  private String generateNewProjectNo() {
    String year = String.valueOf(LocalDate.now().getYear());

    String maxProjNo = projectRepository.findMaxProjnoByYearPrefix(year + "-"); // ex: 2025-003

    int nextSeq = 1;
    if (maxProjNo != null && maxProjNo.length() >= 8) {
      String[] parts = maxProjNo.split("-");
      if (parts.length == 2) {
        try {
          nextSeq = Integer.parseInt(parts[1]) + 1;
        } catch (NumberFormatException ignored) {}
      }
    }

    return String.format("%s-%03d", year, nextSeq); // ex: 2025-004
  }

  @PostMapping("/delete")
  public @ResponseBody AjaxResult deleteData(@RequestParam("projno") String projno,
                                             @RequestParam("spjangcd") String spjangcd) {
    AjaxResult result = new AjaxResult();
    TB_DA003Id id = new TB_DA003Id(spjangcd, projno);

    if (projectRepository.existsById(id)) {
      projectRepository.deleteById(id);
      result.success = true;
      result.message = "삭제 완료";
    } else {
      result.success = (false);
      result.message = ("해당 데이터 없음");
    }

    return result;
  }

}
