package mes.domain.entity;

import java.sql.Timestamp;
import javax.persistence.*;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

@Entity
@Table(name = "tb_as020")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper = false)
public class TbAs020 {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "rptid")
    private Integer rptid; // ID

    @Column(name = "rptdate", nullable = false, length = 10)
    private String rptdate; // 등록일자

    @Column(name = "rptweek", nullable = false, length = 50)
    private String rptweek; // 작성주차

    @Column(name = "frdate", nullable = false, length = 10)
    private String frdate; // 시작일자

    @Column(name = "todate", length = 10)
    private String todate; // 종료일자

    @Column(name = "cltnm", length = 50)
    private String cltnm; // 업체명

    @Column(name = "fixflag", length = 1)
    private String fixflag; // 업무구분

    @Column(name = "actflag", length = 1)
    private String actflag; // 근무구분

    @Column(name = "asmenu", length = 100)
    private String asmenu; // 화면명

    @Column(name = "asdv", length = 1)
    private String asdv; // 요청구분

    @Column(name = "recyn", length = 1)
    private String recyn; // AS진행구분

    @Column(name = "rptremark", length = 500)
    private String rptremark; // 업무내용

    @Column(name = "etcremark", length = 500)
    private String etcremark; // 야근/특근업무

    @Column(name = "remark", length = 100)
    private String remark; // 특이사항

    @Column(name = "fixperid", length = 30)
    private String fixperid; // 본사담당코드

    @Column(name = "fixpernm", length = 30)
    private String fixpernm; // 본사담당자명

    @Column(name = "inputdate")
    private Timestamp inputdate; // 입력일자
}
