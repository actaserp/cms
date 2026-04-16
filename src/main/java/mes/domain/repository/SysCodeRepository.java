package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.SystemCode;

@Repository
public interface SysCodeRepository extends JpaRepository<SystemCode, Integer>{

	SystemCode getSysCodeById(Integer id);

	SystemCode getSysCodeByCodeTypeAndCode(String string, String string2);

	SystemCode findByCodeTypeAndCode(String string, String string2);

	SystemCode findByCodeType(String type);

}
