package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.HaccpDiaryDeviationDetect;

@Repository
public interface HaccpDiaryDeviationDetectRepository extends JpaRepository<HaccpDiaryDeviationDetect, Integer>{

	HaccpDiaryDeviationDetect getHaccpDiaryDeviationDetectById(int hddd_id);
	
	Optional<HaccpDiaryDeviationDetect> getHaccpDiaryDeviationDetectByHaccpTestIdAndHaccpItemId(int haccpTestId, int haccpItemId);
	
	Optional<HaccpDiaryDeviationDetect> findByHaccpTestId(int ht_id);
}
