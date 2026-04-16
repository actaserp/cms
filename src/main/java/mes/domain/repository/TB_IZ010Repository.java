package mes.domain.repository;

import mes.domain.entity.TB_IZ010;
import mes.domain.entity.TB_IZ010Id;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;


import java.util.Optional;

@Repository
public interface TB_IZ010Repository extends JpaRepository<TB_IZ010, TB_IZ010Id> {

  @Query("SELECT t FROM TB_IZ010 t WHERE t.id.spjangcd = :spjangcd AND t.id.cardnum = :cardnum")
  Optional<TB_IZ010> findBySpjangcdAndCardnum(
      @Param("spjangcd") String spjangcd,
      @Param("cardnum") String cardnum
  );
}
