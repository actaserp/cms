package mes.domain.repository;

import mes.domain.entity.MatCompUprice;
import org.springframework.data.domain.Pageable;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

import java.util.List;

public interface MatCompUpriceRepository extends JpaRepository<MatCompUprice, Integer> {

    @Query("SELECT m FROM MatCompUprice m " +
            "WHERE m.companyId = :companyId " +
            "AND m.materialId = :materialId " +
            "AND m.applyEndDate > CURRENT_DATE " +
            "ORDER BY m.applyStartDate DESC"
            )
    List<MatCompUprice> findLastestOne(@Param("companyId") Integer companyId,
                                 @Param("materialId") Integer materialId,
                                 Pageable pageable
                                 );

}