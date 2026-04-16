package mes.domain.entity;


import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.util.ArrayList;
import java.util.List;

@Entity
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Table(name = "tb_salesment")
public class TB_Salesment {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer misnum;;

    private String misdate;
    private String snddate;
    private Integer cltcd;
    private String issuetype;
    private String taxtype;
    private String serialnum;
    private Integer kwon;
    private Integer ho;
    private String purposetype;
    private Integer supplycost;
    private Integer taxtotal;
    private Integer totalamt;
    private Integer cash;
    private Integer chkbill;
    private Integer credit;
    private Integer note;
    private String invoiceetype;

    private String remark1;
    private String remark2;
    private String remark3;

    private String icercorpnum;
    private String icerregid;
    private String icercorpnm;
    private String icerceonm;
    private String iceraddr;
    private String icerbizclass;
    private String icerbiztype;
    private String icerpernm;
    private String icerdeptnm;
    private String icertel;
    private String icerhp;
    private String iceremail;
    private Boolean icersendyn;

    private String ivercorpnum;
    private String iverregid;
    private String ivercorpnm;
    private String iverceonm;
    private String iveraddr;
    private String iverbizclass;
    private String iverbiztype;
    private String iverpernm;
    private String iverdeptnm;
    private String ivertel;
    private String iverhp;
    private String iverpernm2;
    private String iverdeptnm2;
    private String ivertel2;
    private String iverhp2;

    private Integer ivclose;
    private Boolean iversendyn;

    private String ntscfnum;
    private String orgntscfnum;
    private String mgtkey;
    private String orgmgtkey;
    private Integer modifycd;
    private Integer statecode;
    private String statedt;
    private String misgubun;

    private String spjangcd;
    private String iveremail;
    private String issuediv;
    private String ntscode;
    private String accsubcode;
    private String departcode;
    private String projectcode;

    @Version
    private Integer vercode;

    @OneToMany(mappedBy = "salesment", cascade = CascadeType.ALL, orphanRemoval = true)
    private List<TB_SalesDetail> details = new ArrayList<>();
}
