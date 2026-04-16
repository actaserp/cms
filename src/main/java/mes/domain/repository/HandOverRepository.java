package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.HandOver;

public interface HandOverRepository extends JpaRepository<HandOver, Integer>{

	HandOver getHandOverById(Integer id);

	void deleteBySourceDataPkAndSourceTableName(Integer sourceDataPk, String sourceTableName);
	
	HandOver getBySourceDataPk(Integer sourceDataPk);
}
