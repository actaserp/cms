package mes.domain.entity;


import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.Column;
import javax.persistence.EmbeddedId;
import javax.persistence.Entity;
import javax.persistence.Table;

@Setter
@Getter
@Entity
@NoArgsConstructor
@Table(name = "tb_pbcont")
public class Tb_pbcont {


    @EmbeddedId
    private Tb_pbcontId id;

    @Column(name = "sttime")
    String sttime;

    @Column(name = "endtime")
    String endtime;

    @Column(name = "ovsttime")
    String ovsttime;

    @Column(name = "ovedtime")
    String ovedtime;

    @Column(name = "ngsttime")
    String ngsttime;

    @Column(name = "ngedtime")
    String ngedtime;


}
