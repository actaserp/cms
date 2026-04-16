package mes.domain.repository;

import mes.domain.entity.Balju;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.sql.Date;
import java.util.List;

public interface BujuRepository extends JpaRepository<Balju, Integer> {
  Balju getBujuById(Integer id);

  void deleteByJumunNumberAndJumunDateAndSpjangcd(String jumunNumber, Date jumunDate, String spjangcd);

  @Query("SELECT b FROM Balju b WHERE b.BaljuHeadId = :headId")
  List<Balju> findByBaljuHeadId(@Param("headId") Integer headId);

  @Modifying
  @Query("UPDATE Balju s SET s.state = 'force_completion' WHERE s.id IN :ids")
  void forceCompleteBaljuList(@org.apache.ibatis.annotations.Param("ids") List<Integer> ids);
}
