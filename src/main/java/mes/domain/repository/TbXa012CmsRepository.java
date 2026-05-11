package mes.domain.repository;

import mes.domain.entity.Tb_xa012_cms;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;
import java.util.Optional;

public interface TbXa012CmsRepository extends JpaRepository<Tb_xa012_cms, Long> {

    Optional<Tb_xa012_cms> findBySpjangcdAndMsSpjangcdIsNull(String spjangcd);

    Optional<Tb_xa012_cms> findBySpjangcdAndMsSpjangcd(String spjangcd, String msSpjangcd);

    List<Tb_xa012_cms> findBySpjangcd(String spjangcd);

    void deleteBySpjangcd(String spjangcd);
}