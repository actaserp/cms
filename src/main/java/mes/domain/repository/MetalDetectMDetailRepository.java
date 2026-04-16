package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MetalDetectMDetail;

@Repository 
public interface MetalDetectMDetailRepository extends JpaRepository<MetalDetectMDetail, Integer>{

	List<MetalDetectMDetail> findByMetalDetectMasterId(Integer dataPk);

	void deleteByMetalDetectMasterId(Integer metalDetectMasterId);

	MetalDetectMDetail findByMetalDetectMasterIdAndOrder(Integer dataPk, int order);

	MetalDetectMDetail getMetalDetectMDetailById(Integer id);

}
