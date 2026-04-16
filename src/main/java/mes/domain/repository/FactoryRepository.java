package mes.domain.repository;

import mes.domain.entity.Factory;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

@Repository
public interface FactoryRepository extends JpaRepository<Factory, Integer>{

    Factory getFactoryById(Integer id);

}
