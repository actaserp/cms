package mes.domain.repository;

import mes.domain.entity.TB_BANKTRANSIT;
import org.springframework.data.jpa.repository.JpaRepository;

import java.util.List;

public interface TB_BANKTRANSITRepository extends JpaRepository<TB_BANKTRANSIT, Integer> {
    List<TB_BANKTRANSIT> findByTidIn(List<String> tids);
}