package mes.domain.repository;

import mes.domain.entity.TB_DA003;
import mes.domain.entity.TB_DA003Id;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface ProjectRepository extends JpaRepository<TB_DA003, TB_DA003Id> {

  @Query("SELECT MAX(t.id.projno) FROM TB_DA003 t WHERE t.id.projno LIKE CONCAT(:prefix, '%')")
  String findMaxProjnoByYearPrefix(@Param("prefix") String prefix);

  List<TB_DA003> findByIdSpjangcd(String spjangcd);
}