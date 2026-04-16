package mes.domain.repository.approval;

import mes.domain.entity.approval.TB_E063;
import mes.domain.entity.approval.TB_E063_PK;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface E063Repository extends JpaRepository<TB_E063, TB_E063_PK> {


}
