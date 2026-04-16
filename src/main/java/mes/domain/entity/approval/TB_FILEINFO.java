package mes.domain.entity.approval;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Entity
@Table(name="TB_FILEINFO")
@Setter
@Getter
@NoArgsConstructor
public class TB_FILEINFO {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column
    Integer fileseq;     // 순번

    @Column
    String filedate;    // 작성일자
    @Column
    String CHECKSEQ;   // 테이블 인식자 ( 01 : 공지사항 / 02 : 1:1문의  /  03 : 마케팅관리)
    @Column
    int bbsseq;
    @Column
    String FILEPATH;
    @Column
    String FILESVNM;
    @Column
    String FILEEXTNS;
    @Column
    String FILEURL;
    @Column
    String FILEORNM;
    @Column
    BigDecimal FILESIZE;
    @Column
    String FILEREM;
    @Column
    String REPYN;
    @Column
    LocalDateTime INDATEM;
    @Column
    String INUSERID;

}
