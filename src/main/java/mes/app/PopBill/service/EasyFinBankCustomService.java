package mes.app.PopBill.service;


import com.popbill.api.easyfin.EasyFinBankSearchDetail;
import jdk.jshell.execution.Util;
import lombok.extern.slf4j.Slf4j;
import mes.Encryption.EncryptionUtil;
import mes.Exception.EncryptionException;
import mes.app.PopBill.dto.EasyFinBankAccountFormDto;
import mes.sse.Service.SseService;
import mes.sse.SseController;
import mes.app.transaction.service.TransactionInputService;
import mes.app.util.UtilClass;
import mes.domain.entity.TB_ACCOUNT;
import mes.domain.entity.TB_BANKTRANSIT;
import mes.domain.entity.TB_XBANK;
import mes.domain.enums.AccountType;
import mes.domain.repository.TB_ACCOUNTRepository;
import mes.domain.repository.TB_BANKTRANSITRepository;
import mes.domain.repository.TB_XBANKRepository;
import mes.domain.services.SqlRunner;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.SqlParameterSource;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
@Service
public class EasyFinBankCustomService {
    @Autowired
    private TB_BANKTRANSITRepository tB_BANKTRANSITRepository;

    @Autowired
    private TransactionInputService transactionInputService;

    @Autowired
    private com.popbill.api.EasyFinBankService easyFinBankService;

    @Autowired
    SqlRunner sqlRunner;

    @Autowired
    BankTransitTransactionalService bankTransitTransactionalService;

    @Autowired
    TB_XBANKRepository tb_xbankRepository;

    @Autowired
    TB_ACCOUNTRepository accountRepository;

    @Autowired
    SseService sseService;


    //TODO: tid를 기준으로 중복저장 방지함 , tid 목록을 메모리에 적재한다음 유효성 판단할 것
    /**
     * 비동기나 트랜잭션은 프록시 객체에서 처리하는데 Spring이 만들어주는
     * 프록시는 **Spring 컨테이너에서 관리되는 객체(Bean)**에만 적용
     * 그러므로 외부 클래스에서 호출해야 한다.
     *
     * 저장메서드와 비동기 메서드를 클래스별로 분리한 이유 -> Async랑 Transaction 이랑 같이 쓰면 안먹음
     *
     * **/
    @Async
    public void saveBankDataAsync(List<EasyFinBankSearchDetail> list, String jobID, String  accountNumber, Integer accountid, String bankname, String spjangcd){


        List<String> tidList = getTidList(list);
        List<Map<String, Object>> cltCdRelationRemarkList = transactionInputService.getCltCdRelationRemarkList();


        Map<String, TB_BANKTRANSIT> existing = tB_BANKTRANSITRepository.findByTidIn(tidList)
                .stream()
                .collect(Collectors.toMap(TB_BANKTRANSIT::getTid, Function.identity()));
        try{
            List<TB_BANKTRANSIT> tb_banktransitList = new ArrayList<>();

            for(EasyFinBankSearchDetail  map : list){

                String remark = map.getRemark1();
                Map<String, Object> cacheHitItem = findRemarkInList(remark, cltCdRelationRemarkList);

                TB_BANKTRANSIT entity = new TB_BANKTRANSIT();

                String tid = map.getTid();

                if(existing.containsKey(tid)){
                    continue;
                }

                Integer accIn = UtilClass.parseInteger(map.getAccIn());
                String inoutFlag = (accIn == 0) ? "1" : "0";

                entity.setTid(tid);
                entity.setTrdate( map.getTrdate());
                entity.setTrserial(UtilClass.parseInteger(map.getTrserial()));
                entity.setTrdt(map.getTrdt());
                entity.setAccin(accIn);
                entity.setAccout(UtilClass.parseInteger(map.getAccOut()));
                entity.setBalance(UtilClass.parseInteger(map.getBalance()));
                entity.setRemark1(map.getRemark1());
                entity.setRemark2(map.getRemark2());
                entity.setRemark3(map.getRemark3());
                entity.setRemark4(map.getRemark4());
                entity.setRegdt(map.getRegDT());
                entity.setJobid(jobID);
                entity.setIotype("0");
                entity.setMemo(map.getMemo());
                entity.setIoflag(inoutFlag);
                entity.setSpjangcd(spjangcd);
                entity.setAccnum(EncryptionUtil.encrypt(accountNumber));

                entity.setAccid(accountid);
                entity.setBanknm(bankname);

                if(cacheHitItem != null){
                    entity.setCltcd(UtilClass.parseInteger(cacheHitItem.get("cltcd")));
                    entity.setCltflag(UtilClass.getStringSafe(cacheHitItem.get("cltflag")));
                }
                tb_banktransitList.add(entity);

            }
            bankTransitTransactionalService.saveBankDataTransactional(tb_banktransitList);
        }catch(Exception e){
            log.error("비동기 저장 중 예외 발생, {}" ,e.getMessage());
            throw new RuntimeException("비동기 저장 실패", e);
        }
    }

    private Map<String, Object> findRemarkInList(String remark, List<Map<String, Object>> list){
            for(Map<String, Object> item : list){
                String value = UtilClass.getStringSafe(item.get("remark1"));
                if(value.equals(remark)) return item;
            }
            return null;
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW)
    public void saveBankDataSync(List<EasyFinBankSearchDetail> list, String jobID, String  accountNumber, Integer accountid, String bankname, String spjangcd){

        List<Map<String, Object>> cltCdRelationRemarkList = transactionInputService.getCltCdRelationRemarkList();

        final int CHUNK_SIZE = 200;

            List<SqlParameterSource> batchParams = new ArrayList<>();
            String sql = """
                        INSERT INTO TB_BANKTRANSIT (
                            tid, trdate, trserial, trdt, accin, accout, balance,
                            remark1, remark2, remark3, remark4, regdt, jobid, memo,
                            ioflag, accnum, accid, banknm, spjangcd, cltcd, iotype, cltflag
                        ) VALUES (
                            :tid, :trdate, :trserial, :trdt, :accin, :accout, :balance,
                            :remark1, :remark2, :remark3, :remark4, :regdt, :jobid, :memo,
                            :ioflag, :accnum, :accid, :banknm, :spjangcd, :cltcd, '0', :cltflag
                        )
                        ON CONFLICT (tid) DO NOTHING
                        """;

                for(EasyFinBankSearchDetail  map : list){

                    try{
                        String tid = map.getTid();
                        String remark = map.getRemark1();

                        Map<String, Object> remarkInList = findRemarkInList(remark, cltCdRelationRemarkList);


                        Integer accIn = UtilClass.parseInteger(map.getAccIn());
                        String inoutFlag = (accIn == 0) ? "1" : "0";
                        String EncryptedAccountNum = EncryptionUtil.encrypt(accountNumber);

                        MapSqlParameterSource param = new MapSqlParameterSource()
                                .addValue("tid", tid)
                                .addValue("trdate", map.getTrdate())
                                .addValue("trserial", UtilClass.parseInteger(map.getTrserial()))
                                .addValue("trdt", map.getTrdt())
                                .addValue("accin", accIn)
                                .addValue("accout", UtilClass.parseInteger(map.getAccOut()))
                                .addValue("balance", UtilClass.parseInteger(map.getBalance()))
                                .addValue("remark1", map.getRemark1())
                                .addValue("remark2", map.getRemark2())
                                .addValue("remark3", map.getRemark3())
                                .addValue("remark4", map.getRemark4())
                                .addValue("regdt", map.getRegDT())
                                .addValue("jobid", jobID)
                                .addValue("memo", map.getMemo())
                                .addValue("ioflag", inoutFlag)
                                .addValue("accnum", EncryptedAccountNum)
                                .addValue("accid", accountid)
                                .addValue("banknm", bankname)
                                .addValue("spjangcd", spjangcd)
                                .addValue("cltcd", remarkInList.getOrDefault("cltcd", null))
                                .addValue("cltflag", remarkInList.getOrDefault("cltflag", null));


                        batchParams.add(param);

                        if (batchParams.size() >= CHUNK_SIZE) {
                            try {
                                log.info("[쿼리 실행 - {}건] : {}", batchParams.size(), sql);

                                sqlRunner.batchUpdate(sql, batchParams.toArray(new SqlParameterSource[0]));

                                sseService.sendSystem(spjangcd, accountNumber);

                            } catch (Exception e) {
                                log.warn("배치 저장 중 일부 오류 발생 (무시됨): {}", e.getMessage());
                            }
                            batchParams.clear();
                        }
                    }catch(Exception e){
                        log.warn("단일 데이터 변환/저장 중 오류 발생 (무시됨): {}", e.getMessage());
                    }
                }

                if (!batchParams.isEmpty()) {
                    try {
                        log.info("[쿼리 실행 - {}건] : {}", batchParams.size(), sql);

                        sqlRunner.batchUpdate(sql, batchParams.toArray(new SqlParameterSource[0]));

                        sseService.sendSystem(spjangcd, accountNumber);
                    } catch (Exception e) {
                        log.warn("마지막 배치 저장 중 일부 오류 발생 (무시됨): {}", e.getMessage());
                    }
                }

    }

    public List<String> getTidList(List<EasyFinBankSearchDetail> list){

        return list.stream()
                .map(dto -> dto.getTid())
                .collect(Collectors.toList());

    }

    public List<Map<String, Object>> convertToMapList(List<EasyFinBankSearchDetail> detailList) {
        List<Map<String, Object>> result = new ArrayList<>();

        for (EasyFinBankSearchDetail detail : detailList) {
            Map<String, Object> map = new LinkedHashMap<>(); // 순서 유지

            map.put("tid", detail.getTid());  //거래내역아이디
            map.put("accountID", detail.getAccountID()); //
            map.put("trdate", detail.getTrdate().substring(0, 4) + "-" + detail.getTrdate().substring(4, 6) + "-" + detail.getTrdate().substring(6, 8)); //거래일자
            map.put("trserial", detail.getTrserial()); //거래일련번호
            map.put("trdt", detail.getTrdt()); //거래일시
            map.put("accin",  UtilClass.parseInteger(detail.getAccIn())); //입금액
            map.put("accout", detail.getAccOut()); //출금액
            map.put("balance", detail.getBalance()); //잔액
            map.put("remark1", detail.getRemark1() + detail.getRemark2() + detail.getRemark3() + detail.getRemark4());
            map.put("regdt", detail.getRegDT()); //등록일시
            map.put("memo", detail.getMemo()); //메모

            result.add(map);
        }

        return result;
    }


    @Transactional
    public void saveRegistAccount(EasyFinBankAccountFormDto form, TB_ACCOUNT account){

        try{
            TB_XBANK bank = tb_xbankRepository.findByBankPopCd(form.getBankName());


            String accountType = form.getAccountType();
            String accountnum = form.getAccountNumber();
            String accountPw = form.getPaymentPw();
            String viewpw = form.getViewpw();

            account.setBankid(bank.getBankId());
            account.setAccnum(EncryptionUtil.encrypt(accountnum));
            account.setAccname(form.getAccountAlias());
            account.setOnlineid(form.getBankId());
            account.setPopsort(AccountType.fromCode(form.getAccountType()).orElseThrow(
                    () -> new IllegalArgumentException("올바르지 않은 계좌유형입니다."))
                    .getValue()
            );
            account.setPopyn("1");

            if(accountType.equals(AccountType.PERSON.getCode())) { //개인
                account.setAccbirth(form.getIdentityNumber());
            }

            account.setAccpw(EncryptionUtil.encrypt(accountPw));
            account.setViewid(form.getViewid());
            account.setViewpw(EncryptionUtil.encrypt(viewpw));

            accountRepository.save(account);
        }catch(Exception e){
            log.error("팝빌연동 계좌 등록중 오류 발생", e);
            throw new EncryptionException("계좌 관련 암호화 중 오류가 발생");
        }
    }
}
