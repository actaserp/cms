package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.JobResDefect;

@Repository
public interface JobResDefectRepository extends JpaRepository<JobResDefect, Integer>{

	List<JobResDefect> findByJobResponseId(Integer jrPk);
	void deleteByJobResponseId(Integer jobResponseId);
	JobResDefect findByJobResponseIdAndDefectTypeId(Integer jrPk, int defectTypeId);

}
