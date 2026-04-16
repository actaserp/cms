package mes.domain.repository.mobile;

import mes.domain.entity.mobile.TB_PB204;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

@Repository
public interface TB_PB204Repository extends JpaRepository<TB_PB204, Integer> {

    // 퇴근등록로직 중 유연근무제 신청 여부 확인
    @Query("""
        SELECT t 
        FROM TB_PB204 t 
        WHERE t.personid = :personid 
          AND :today BETWEEN t.frdate AND t.todate 
          AND t.workcd = :workcd
    """)
    TB_PB204 findFlexibleWorkByPersonAndDate(
            @Param("personid") Integer personid,
            @Param("today") String today,
            @Param("workcd") String workcd
    );

    // 개인별 휴가현황조회 조회년도 바인드 로직
    @Query("""
        SELECT DISTINCT SUBSTRING(t.frdate, 1, 4)
        FROM TB_PB204 t
        WHERE t.personid = :personid
        ORDER BY SUBSTRING(t.frdate, 1, 4) DESC
    """)
    List<String> findDistinctYearsByPersonId(@Param("personid") Integer personid);
}
