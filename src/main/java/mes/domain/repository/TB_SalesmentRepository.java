package mes.domain.repository;

import mes.domain.entity.TB_Salesment;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;
import java.util.Optional;

@Repository
public interface TB_SalesmentRepository extends JpaRepository<TB_Salesment, Integer>  {
    @EntityGraph(attributePaths = "details")
    List<TB_Salesment> findAllByMisnumIn(List<Integer> misnums);

    Optional<TB_Salesment> findByMgtkeyAndNtscfnum(String mgtkey, String ntscfnum);
}