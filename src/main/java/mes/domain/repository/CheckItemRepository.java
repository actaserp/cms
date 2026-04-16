package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.CheckItem;

@Repository
public interface CheckItemRepository extends JpaRepository<CheckItem, Integer>{

	List<CheckItem> findByCode(String itemCode);

	List<CheckItem> findByIdNotIn(List<Integer> list);

	CheckItem getCheckItemById(Integer id);

	List<CheckItem> findBycheckMasterId(Integer check_id);

	CheckItem findByCheckMasterIdAndCode(Integer id, String Code);
}
