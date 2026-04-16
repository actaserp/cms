package mes.domain.entity;


import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;

import javax.persistence.*;

@Entity
@Table(name="tb_yearamt")
@NoArgsConstructor
@Data
@EqualsAndHashCode( callSuper=false)
public class Yearamt {

    @EmbeddedId
    private YearamtId id;

    @Column(name = "yearamt")
    Integer yearamt; //마감금액

    @Column(name = "endyn")
    String endyn;  // 마감유무

    @Column(name="spjangcd")
    String spjangcd;

}