package mes.app.Scheduler.SchedulerService;


import com.popbill.api.EasyFinBankService;
import com.popbill.api.PopbillException;
import com.popbill.api.easyfin.EasyFinBankJobState;
import com.popbill.api.easyfin.EasyFinBankSearchDetail;
import com.popbill.api.easyfin.EasyFinBankSearchResult;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import mes.Encryption.EncryptionUtil;
import mes.app.PopBill.enums.BankJobState;
import mes.app.PopBill.service.EasyFinBankCustomService;
import mes.sse.SseController;
import mes.app.util.UtilClass;
import mes.domain.model.AjaxResult;
import mes.domain.services.SqlRunner;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.Map;
import java.util.List;

@Service
@Slf4j
@RequiredArgsConstructor
public class AccountSyncService {

    private final SseController sseController;
    private final SqlRunner sqlRunner;
    private final EasyFinBankService easyFinBankService;
    private final EasyFinBankCustomService easyFinBankCustomService;


    public void run() {

        log.info("[{}] 시작 - Thread: {}", "계좌수집", Thread.currentThread().getName());

        LocalDateTime startTime = LocalDateTime.now();

        List<Map<String, Object>> accountList = getAccountList();

        for (Map<String, Object> account : accountList) {
            try {
                Map<String, String> params = extractParameters(account);
                String corpNum = params.get("CorpNum");

                AjaxResult requestResult = requestJob(params);
                String jobId = (String) requestResult.data;
                Integer accountId = UtilClass.parseInteger(account.get("AccountId"));

                if (jobId == null) {
                    log.error("팝빌 계좌 수집 요청 실패: {}, 계좌 ID: {}", requestResult.message, accountId);
                    continue;
                }

                if (!waitForJobComplete(corpNum, jobId).equals(BankJobState.COMPLETE.getCode())) {
                    log.warn("수집 작업이 완료되지 않음. 계좌 ID: {}", accountId);
                    continue;
                }

                EasyFinBankSearchResult searchResult = easyFinBankService.search(corpNum, jobId, null, null, null, null, null);

                if (searchResult.getCode() != 1) {
                    log.error("팝빌 계좌 수집 실패: {}, 계좌 ID: {}", searchResult.getMessage(), accountId);
                    continue;
                }

                List<EasyFinBankSearchDetail> list = searchResult.getList();
                //동기로 DB에 저장
                easyFinBankCustomService.saveBankDataSync(list, jobId, params.get("AccountNumber"), accountId, params.get("BankName"), params.get("spjangcd"));

            } catch (Exception e) {
                log.error("계좌 복호화 또는 수집 중 예외 발생: {}, 계좌번호: {}", e.getMessage(), account.get("AccountNumber"));
            }
        }

        LocalDateTime endTime = LocalDateTime.now();
        Duration duration = Duration.between(startTime, endTime);
        //log.info("총 계좌 수집 소요 시간 : {} 초", duration.getSeconds());
    }

    private List<Map<String, Object>> getAccountList() {
        String sql = """
            SELECT a.accid AS "AccountId",
                   b.saupnum AS "CorpNum",
                   c.bankpopcd AS "BankCode",
                   c.banknm AS "BankName",
                   a.accnum   AS "AccountNumber",
                   a.spjangcd AS "spjangcd"
              FROM tb_account a
              LEFT JOIN tb_xa012 b ON b.spjangcd = a.spjangcd
              LEFT JOIN tb_xbank c ON a.bankid = c.bankid
             WHERE a.popyn = '1'
        """;

        return sqlRunner.getRows(sql, new MapSqlParameterSource());
    }

    private Map<String, String> extractParameters(Map<String, Object> account) throws Exception {
        String today = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        String raw = UtilClass.getStringSafe(account.get("AccountNumber"));
        String spjangcd = UtilClass.getStringSafe(account.get("spjangcd"));

        Map<String, String> param = new HashMap<>();
        param.put("CorpNum", UtilClass.getStringSafe(account.get("CorpNum")));
        param.put("BankCode", UtilClass.getStringSafe(account.get("BankCode")));
        param.put("AccountNumber", EncryptionUtil.decrypt(raw));
        param.put("SDate", today);
        param.put("EDate", today);
        param.put("spjangcd", spjangcd);

        return param;
    }

    private AjaxResult requestJob(Map<String, String> param) {
        AjaxResult result = new AjaxResult();
        try {
            String jobId = easyFinBankService.requestJob(
                    param.get("CorpNum"),
                    param.get("BankCode"),
                    param.get("AccountNumber"),
                    param.get("SDate"),
                    param.get("EDate")
            );
            result.success = true;
            result.data = jobId;
        } catch (PopbillException e) {
            result.success = false;
            result.message = e.getMessage();
        }
        return result;
    }

    private String waitForJobComplete(String corpNum, String jobId) throws InterruptedException {
        final int maxRetry = 10;
        final int interval = 1000;

        try {
            for (int i = 0; i < maxRetry; i++) {
                EasyFinBankJobState jobState = easyFinBankService.getJobState(corpNum, jobId);
                String jobStateCode = jobState.getJobState(); // 1=대기, 2=진행중, 3=완료
                long errorCode = jobState.getErrorCode();

                if(errorCode != 1 && errorCode != 0){
                    log.info("에러코드 발생 {}" ,errorCode);
                    return "에러발생";
                }

                BankJobState state = BankJobState.fromCode(jobStateCode);

                if(state == BankJobState.COMPLETE){
                    log.info("수집완료");
                    return BankJobState.COMPLETE.getCode();
                }

                Thread.sleep(interval);
            }
        } catch (PopbillException e) {
            log.error("팝빌 수집 상태 확인 중 예외 발생: {}, Job ID: {}", e.getMessage(), jobId);
        }

        return BankJobState.TIMEOUT.getCode();
    }
}
