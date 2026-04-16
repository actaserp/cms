package mes.app.PopBill.service;

import lombok.RequiredArgsConstructor;
import mes.domain.entity.TB_BANKTRANSIT;
import mes.domain.repository.TB_BANKTRANSITRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

@Service
@RequiredArgsConstructor
public class BankTransitTransactionalService {

    private final TB_BANKTRANSITRepository tbBanktransitRepository;

    @Transactional
    public void saveBankDataTransactional(List<TB_BANKTRANSIT> list){
        tbBanktransitRepository.saveAll(list);
    }

    public List<TB_BANKTRANSIT> getSavedBankTransitList(List<String> tids){
        return tbBanktransitRepository.findByTidIn(tids);
    }

}
