package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.EduResult;

@Repository
public interface EduResultRepository extends JpaRepository<EduResult, Integer>{

	EduResult getEduResultById(Integer id);

	List<EduResult> findBySourceDataPkAndSourceTableName(Integer bhId, String string);

}
