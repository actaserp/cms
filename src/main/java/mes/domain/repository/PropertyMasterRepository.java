package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.PropertyMaster;

@Repository
public interface PropertyMasterRepository extends JpaRepository<PropertyMaster, Integer>{

	PropertyMaster getPropMasterById(Integer id);
	
	List<PropertyMaster> findByTableNameAndCode(String TableName, String Code);
}
