package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.ApprResult;

public interface ApprResultRepository extends JpaRepository<ApprResult, Integer>{

	ApprResult getApprResultById(Integer id);

	ApprResult findApprResultById(Integer headId);
	
	ApprResult getBySourceDataPkAndSourceTableNameAndApproverIdAndApprStepYN(Integer SourceDataPk,
			String sourceTableName, Integer approverId, String apprStepYN);
}
