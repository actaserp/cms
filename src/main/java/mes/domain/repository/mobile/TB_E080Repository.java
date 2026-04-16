package mes.domain.repository.mobile;

import mes.domain.entity.approval.TB_E080;
import mes.domain.entity.approval.TB_E080_PK;
import mes.domain.entity.mobile.TB_PB204;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface TB_E080Repository extends JpaRepository<TB_E080, TB_E080_PK> {
}
