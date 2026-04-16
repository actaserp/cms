package mes.domain.repository;

import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import mes.domain.entity.HaccpProcess;

public interface HaccpProcessRepository extends JpaRepository<HaccpProcess, Integer> {
	
	HaccpProcess getHaccpProcessById(Integer id);

	Optional<HaccpProcess> findByCode(String code);
}
