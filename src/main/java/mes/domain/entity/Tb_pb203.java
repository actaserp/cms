package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;
import java.math.BigDecimal;

@Setter
@Getter
@Entity
@NoArgsConstructor
@Table(name = "tb_pb203")
public class Tb_pb203 {

    @EmbeddedId
    private Tb_pb203Id id;

    @Column(name = "workday")
    Integer workday;

    @Column(name = "worktime")
    BigDecimal worktime;

    @Column(name = "nomaltime")
    BigDecimal nomaltime;

    @Column(name = "overtime")
    BigDecimal overtime;

    @Column(name = "nighttime")
    BigDecimal nighttime;

    @Column(name = "holitime")
    BigDecimal holitime;

    @Column(name = "jitime")
    BigDecimal jitime;

    @Column(name = "jotime")
    BigDecimal jotime;

    @Column(name = "yuntime")
    BigDecimal yuntime;

    @Column(name = "abtime")
    BigDecimal abtime;

    @Column(name = "bantime")
    BigDecimal bantime;

    @Column(name = "adttime01")
    BigDecimal adttime01;

    @Column(name = "adttime02")
    BigDecimal adttime02;

    @Column(name = "adttime03")
    BigDecimal adttime03;

    @Column(name = "adttime04")
    BigDecimal adttime04;

    @Column(name = "adttime05")
    BigDecimal adttime05;

    @Column(name = "adttime06")
    BigDecimal adttime06;

    @Column(name = "adttime07")
    BigDecimal adttime07;

    @Column(name = "remark")
    String remark;

    @Column(name = "fixflag")
    String fixflag;

}
