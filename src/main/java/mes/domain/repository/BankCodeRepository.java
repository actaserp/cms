package mes.domain.repository;

import mes.domain.entity.BankCode;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.List;

@Repository
public interface BankCodeRepository extends JpaRepository<BankCode, Integer> {
	List<BankCode> findByName(String name);
	BankCode getBankCodeById(Integer id);
}
