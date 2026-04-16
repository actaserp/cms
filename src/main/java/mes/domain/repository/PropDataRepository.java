package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.PropData;

public interface PropDataRepository extends JpaRepository<PropData, Integer> {

	List<PropData> findByTableNameAndDataPkAndCode(String tableName, Integer id, String code);
	
	PropData findByDataPkAndTableNameAndCode(String tableName, Integer id, String code);
	
	void deleteByDataPk(Integer id);

	PropData findByDataPkAndTableNameAndCode(Integer check_result_id, String string, String string2);

}
