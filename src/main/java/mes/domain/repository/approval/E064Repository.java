package mes.domain.repository.approval;

import mes.domain.entity.approval.TB_E064;
import mes.domain.entity.approval.TB_E064_PK;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

@Repository
public interface E064Repository extends JpaRepository<TB_E064, TB_E064_PK> {
    @Query("SELECT MAX(e.id.no) FROM TB_E064 e WHERE e.id.spjangcd = :spjangcd AND e.id.personid = :personid AND e.id.papercd = :papercd")
    String findMaxNo(@Param("spjangcd") String spjangcd,
                     @Param("personid") Integer personid,
                     @Param("papercd") String papercd);

}
