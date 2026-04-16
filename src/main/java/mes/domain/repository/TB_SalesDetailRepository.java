package mes.domain.repository;

import mes.domain.entity.TB_SalesDetail;
import mes.domain.entity.TB_SalesDetailId;
import org.apache.ibatis.annotations.Param;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import javax.transaction.Transactional;
import java.util.List;

@Repository
public interface TB_SalesDetailRepository extends JpaRepository<TB_SalesDetail, TB_SalesDetailId>  {


    @Modifying
    @Transactional
    @Query("DELETE FROM TB_SalesDetail d WHERE d.id.misnum = :misnum")
    void deleteByMisnum(@Param("misnum") Integer misnum);

    @Query("SELECT d FROM TB_SalesDetail d WHERE d.id.misnum = :misnum")
    List<TB_SalesDetail> findByMisnum(@Param("misnum") Integer misnum);



}