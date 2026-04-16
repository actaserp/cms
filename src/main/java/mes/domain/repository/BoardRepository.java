package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Board;

@Repository
public interface BoardRepository extends JpaRepository<Board, Integer> {
	
	Board getBoardById(Integer id);
}
