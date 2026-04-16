package mes.domain.repository;

import mes.domain.entity.TB_InvoiceDetail;
import mes.domain.entity.TB_InvoiceDetailId;
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
public interface TB_InvoiceDetailRepository extends JpaRepository<TB_InvoiceDetail, TB_InvoiceDetailId>  {


    @Modifying
    @Transactional
    @Query("DELETE FROM TB_InvoiceDetail  d WHERE d.id.misnum = :misnum")
    void deleteByMisnum(@Param("misnum") Integer misnum);

    @Query("SELECT d FROM TB_InvoiceDetail d WHERE d.id.misnum = :misnum")
    List<TB_InvoiceDetail> findByMisnum(@Param("misnum") Integer misnum);



}