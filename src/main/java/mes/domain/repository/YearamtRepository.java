package mes.domain.repository;

import mes.domain.entity.Yearamt;
import mes.domain.entity.YearamtId;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.Optional;

@Repository
public interface YearamtRepository extends JpaRepository<Yearamt, YearamtId> {

  void deleteByIdIoflagAndIdYyyymmAndIdCltcdAndSpjangcd(String ioflag, String yyyymm, Integer cltcd, String spjangcd);
}
