package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.ActionData;

@Repository 
public interface ActionDataRepository extends JpaRepository<ActionData, Integer> {
	
	ActionData getActionDataById(Integer id);

	List<ActionData> findByDataPk2AndTableName2(Integer bhId, String string);
	
}