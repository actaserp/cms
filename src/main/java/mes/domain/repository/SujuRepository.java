package mes.domain.repository;

//import java.util.List;

import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import mes.domain.entity.Suju;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;
@Repository 
public interface SujuRepository extends JpaRepository<Suju, Integer>{

	
	Suju getSujuById(Integer id);

	Suju findByIdAndState(Integer sujuPk, String string);

	@Transactional(readOnly = true)
    List<Suju> findByIdIn(List<Integer> ids);

	@Modifying
	@Query("DELETE FROM Suju s WHERE s.sujuHeadId = :sujuHeadId")
	void deleteBySujuHeadId(@Param("sujuHeadId") Integer sujuHeadId);

	@Modifying
	@Query("UPDATE Suju s SET s.state = 'force_completion' WHERE s.id IN :ids")
	void forceCompleteSujuList(@Param("ids") List<Integer> ids);
}
