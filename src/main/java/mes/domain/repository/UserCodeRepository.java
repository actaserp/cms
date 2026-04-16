package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.UserCode;

public interface UserCodeRepository extends JpaRepository<UserCode, Integer>{

	List<UserCode> findByCodeAndParentIdIsNull(String parentCode);

	List<UserCode> findByCodeAndParentId(String code, Integer parentId);

	UserCode getUserCodeById(Integer id);
	
	List<UserCode> findByCode (String code);
	
		

}
