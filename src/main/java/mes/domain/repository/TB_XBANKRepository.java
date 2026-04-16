package mes.domain.repository;

import mes.domain.entity.TB_XBANK;
import org.springframework.data.jpa.repository.JpaRepository;

public interface TB_XBANKRepository extends JpaRepository<TB_XBANK, Integer> {
    TB_XBANK findByBankPopCd(String number);

}