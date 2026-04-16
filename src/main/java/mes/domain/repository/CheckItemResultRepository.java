package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.CheckItemResult;


public interface CheckItemResultRepository extends JpaRepository<CheckItemResult, Integer> {
	
	CheckItemResult getCheckItemResultById(Integer id);

	List<CheckItemResult> findByCheckResultId(Integer check_result_id);
	
	CheckItemResult findByCheckResultIdAndCheckItemIdAndResult3(Integer check_result_id,Integer itemId, String k);

	CheckItemResult findByCheckResultIdAndCheckItemId(Integer check_result_id, Integer check_item_id);

	List<CheckItemResult> findByCheckItemId(Integer id);
	
	void deleteByCheckResultId(Integer check_result_id);
}
