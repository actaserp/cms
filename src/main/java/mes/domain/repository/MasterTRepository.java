package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.MasterT;

@Repository 
public interface MasterTRepository extends JpaRepository<MasterT, Integer> {

	Optional<MasterT> findByCodeAndMasterClass(String code, String masterClass);

	MasterT getMasterTById(Integer id);

	MasterT findByIdAndMasterClass(int masterId, String masterClass);
}
