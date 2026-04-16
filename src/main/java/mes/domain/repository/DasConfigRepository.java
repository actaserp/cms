package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.DasConfig;

@Repository
public interface DasConfigRepository extends JpaRepository<DasConfig, Integer>{

	DasConfig getDasConfigById(Integer id);

}
