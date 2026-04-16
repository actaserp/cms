package mes.domain.entity.approval;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Entity
@Table(name = "tb_e080") // DB 테이블명과 매핑
@Setter
@Getter
@NoArgsConstructor
public class TB_E080 {

    @EmbeddedId
    private TB_E080_PK id;

    @Column(length = 1)
    private String flag;

    @Column(length = 8)
    private String repodate;

    @Column(length = 3)
    private String papercd;

    @Column
    private Integer repoperid;

    @Column(length = 50)
    private String title;

    @Column(length = 3)
    private String appgubun;

    @Column
    private Integer inperid;

    @Column(length = 8)
    private String indate;

    @Column(length = 1)
    private String adflag;

    @Column(length = 30)
    private String prtflag;

    @Column(length = 2)
    private String gubun;
}
