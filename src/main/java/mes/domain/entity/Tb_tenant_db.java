package mes.domain.entity;

import lombok.Data;

import javax.persistence.*;

@Data
@Entity
@Table(name = "tb_tenant_db")
public class Tb_tenant_db {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Integer id;

    @Column(name = "spjangcd", nullable = false, length = 10)
    private String spjangcd;

    @Column(name = "db_alias", nullable = false, length = 50)
    private String dbAlias;

    @Column(name = "db_url", nullable = false, length = 255)
    private String dbUrl;

    @Column(name = "db_username", length = 100)
    private String dbUsername;

    @Column(name = "db_password", length = 255)
    private String dbPassword;

    @Column(name = "db_type", nullable = false, length = 20)
    private String dbType;

    @Column(name = "pool_size")
    private Integer poolSize;

    @Column(name = "description", length = 200)
    private String description; // schema 저장 용도로 활용
}