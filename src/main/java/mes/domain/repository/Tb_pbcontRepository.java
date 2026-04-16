package mes.domain.repository;

import mes.domain.entity.Tb_pbcont;
import mes.domain.entity.Tb_pbcontId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface Tb_pbcontRepository extends JpaRepository<Tb_pbcont, Tb_pbcontId> {

}
