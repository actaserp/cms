package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MetalDetectM;

@Repository 
public interface MetalDetectMRepository extends JpaRepository<MetalDetectM, Integer>{

	MetalDetectM getMetalDetectMById(Integer id);

}
