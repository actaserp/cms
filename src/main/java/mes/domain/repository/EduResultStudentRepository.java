package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.EduResultStudent;

@Repository
public interface EduResultStudentRepository extends JpaRepository<EduResultStudent, Integer>{

	List<EduResultStudent> findByEduResultId(int id);

	List<EduResultStudent> findBySourceDataPkAndSourceTableName(Integer bhId, String string);

}
