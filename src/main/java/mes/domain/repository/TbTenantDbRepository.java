package mes.domain.repository;

import mes.domain.entity.Tb_tenant_db;
import mes.domain.entity.Tb_xa012_cms;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TbTenantDbRepository extends JpaRepository<Tb_tenant_db, Integer> {

    Optional<Tb_tenant_db> findBySpjangcdAndDbAlias(String spjangcd, String dbAlias);

    List<Tb_tenant_db> findBySpjangcd(String spjangcd);
}