package mes.domain.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "tb_xa012_cms")
public class Tb_xa012_cms {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "spjangcd", nullable = false, length = 2)
    private String spjangcd;

    @Column(name = "ms_spjangcd", length = 2)
    private String msSpjangcd;

    @Column(name = "cms_code", nullable = false, length = 10)
    private String cmsCode;

    @Column(name = "cms_password", length = 100)
    private String cmsPassword;

    @Column(name = "cms_state", length = 10)
    private String cmsState;

    @Column(name = "cms_bank_code", length = 3)
    private String cmsBankCode;

    @Column(name = "cms_recv_account", length = 16)
    private String cmsRecvAccount;

    @Column(name = "cms_bank_branch", length = 7)
    private String cmsBankBranch;

    @Column(name = "cms_description", length = 30)
    private String cmsDescription;
}