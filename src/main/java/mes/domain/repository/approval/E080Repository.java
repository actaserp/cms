package mes.domain.repository.approval;

import mes.domain.entity.approval.TB_E080;
import mes.domain.entity.approval.TB_E080_PK;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface E080Repository extends JpaRepository<TB_E080, TB_E080_PK> {


}
