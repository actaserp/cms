package mes.domain.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.SeqMaker;

@Repository
public interface SeqMakerRepository extends JpaRepository<SeqMaker, Integer>{

	List<SeqMaker> findByCodeAndBaseDate(String string, String format);

}
