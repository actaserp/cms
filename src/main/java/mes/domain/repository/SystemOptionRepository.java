package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.SystemOption;

@Repository 
public interface SystemOptionRepository extends JpaRepository<SystemOption, Integer> {

	public SystemOption getByCode(String code);	
}
