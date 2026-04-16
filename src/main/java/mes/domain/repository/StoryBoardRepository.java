package mes.domain.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import mes.domain.entity.StoryBoardItem;

@Repository
public interface StoryBoardRepository extends JpaRepository<StoryBoardItem, Integer>{

	StoryBoardItem getStoryBoardById(Integer id);

}
