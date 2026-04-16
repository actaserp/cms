package mes.domain.entity;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;
import java.math.BigDecimal;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "tb_xa012", schema = "public")
public class Tb_xa012 {

    @Id
    @Column(name = "spjangcd", nullable = false, length = 2)
    private String spjangcd;

    @Column(name = "saupnum", length = 15)
    private String saupnum;

    @Column(name = "spjangnm", length = 100)
    private String spjangnm;

    @Column(name = "compnum", length = 15)
    private String compnum;

    @Column(name = "prenm", length = 30)
    private String prenm;

    @Column(name = "zipcd", length = 5)
    private String zipcd;

    @Column(name = "adresa", length = 100)
    private String adresa;

    @Column(name = "adresb", length = 100)
    private String adresb;

    @Column(name = "zipcd2", length = 5)
    private String zipcd2;

    @Column(name = "adres2a", length = 100)
    private String adres2a;

    @Column(name = "adres2b", length = 100)
    private String adres2b;

    @Column(name = "biztype", length = 50)
    private String biztype;

    @Column(name = "item", length = 50)
    private String item;

    @Column(name = "tel1", length = 25)
    private String tel1;

    @Column(name = "tel2", length = 25)
    private String tel2;

    @Column(name = "fax", length = 25)
    private String fax;

    @Column(name = "emailadres", length = 50)
    private String emailadres;

    @Column(name = "agnertel1", length = 20)
    private String agnertel1;

    @Column(name = "agnertel2", length = 25)
    private String agnertel2;

    @Column(name = "comtaxoff", length = 10)
    private String comtaxoff;

    @Transient
    private String taxnm;

    @Column(name = "custperclsf", length = 1)
    private String custperclsf;

    @Column(name = "taxagentnm", length = 20)
    private String taxagentnm;

    @Column(name = "taxagentcd", length = 20)
    private String taxagentcd;

    @Column(name = "taxagenttel", length = 25)
    private String taxagenttel;

    @Column(name = "taxagentsp", length = 20)
    private String taxagentsp;

    @Column(name = "taxaccnum", length = 20)
    private String taxaccnum;

    @Column(name = "popidmsg", length = 20)
    private String popidmsg;

    @Column(name = "poppw", length = 20)
    private String poppw;

    @Column(name = "invoicertaxregid", length = 4)
    private String invoicertaxregid;

    @Column(name = "ajongcd", length = 3)
    private String ajongcd;

    @Column(name = "openymd", length = 8)
    private String openymd;

    @Column(name = "eddate", length = 8)
    private String eddate;

    @Column(name = "subscribe", length = 20)
    private String subscribe;

    @Column(name = "subscribeunit", length = 20)
    private String subscribeunit;

    @Column(name = "state", length = 20)
    private String state;

    @Column(name = "billingdate")
    private Integer billingdate;

    @Column(name = "expirationdate", length = 8)
    private String expirationdate;

    @Column(name = "chargeamount", precision = 15, scale = 2)
    private BigDecimal chargeamount;

    @Column(name = "subscriptiondate", length = 8)
    private String subscriptiondate;

    @Column(name = "bill_plans_id")
    private Integer bill_plans_id;
}
