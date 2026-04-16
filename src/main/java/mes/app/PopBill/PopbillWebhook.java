package mes.app.PopBill;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import mes.domain.entity.TB_Salesment;
import mes.domain.repository.TB_SalesmentRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.relational.core.sql.In;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.util.Map;
import java.util.Optional;

@RestController
@RequestMapping("/popbill")
public class PopbillWebhook {

    @Autowired
    private TB_SalesmentRepository tb_salesmentRepository;

    private static final Logger logger = LoggerFactory.getLogger(PopbillWebhook.class);
    private final ObjectMapper objectMapper = new ObjectMapper();

    @PostMapping("/webhook")
    public String webhook(HttpServletRequest request) {
        System.out.println("==== 웹훅 호출됨 ====");
        StringBuilder strBuffer = new StringBuilder();

        try (BufferedReader reader = request.getReader()) {
            String line;
            while ((line = reader.readLine()) != null) {
                strBuffer.append(line);
            }
        } catch (Exception e) {
            logger.error("Error reading JSON string: ", e);
            return "{\"result\":\"FAIL\",\"message\":\"" + e.getMessage() + "\"}";
        }

        String requestBody = strBuffer.toString();
        logger.info("Received Popbill Webhook: {}", requestBody);

        try {
            Map<String, Object> jsonMap = objectMapper.readValue(requestBody, new TypeReference<>() {});
            String eventType = (String) jsonMap.get("eventType");

            switch (eventType) {
                case "CLOSEDOWN":
                    handleClosedown(jsonMap);
                    break;
                case "NTS":
                    handleNtsStatus(jsonMap);
                    break;
                case "Request":
                    handleReverseRequest(jsonMap);
                    break;
                case "CancelRequest":
                    handleCancelReverseRequest(jsonMap);
                    break;
                case "Refuse":
                    handleRefuseReverseRequest(jsonMap);
                    break;
                default:
                    logger.warn("Unknown eventType: {}", eventType);
                    break;
            }

        } catch (Exception e) {
            logger.error("Error parsing JSON: ", e);
            return "{\"result\":\"FAIL\",\"message\":\"Invalid JSON format\"}";
        }

        // Webhook 수신 성공 응답
        return "{\"result\":\"OK\"}";
    }

    private void handleClosedown(Map<String, Object> data) {
        logger.info("공급받는자 사업자등록상태조회 결과 처리");
        String mgtKey = (String) data.get("invoicerMgtKey");
        String ntscfnum = (String) data.get("ntsconfirmNum");
        Integer closeDownState = (Integer) data.get("closeDownState");

        Optional<TB_Salesment> optional = tb_salesmentRepository.findByMgtkeyAndNtscfnum(mgtKey, ntscfnum);

        if (optional.isPresent()) {
            TB_Salesment salesment = optional.get();
            salesment.setIvclose(closeDownState);

            tb_salesmentRepository.save(salesment);
            logger.info("ivclose 상태 업데이트 완료: {} → {}", mgtKey, closeDownState);
        } else {
            logger.warn("세금계산서 매칭 실패 - mgtKey: {}, ntsconfirmNum: {}", mgtKey, ntscfnum);
        }
    }

    private void handleNtsStatus(Map<String, Object> data) {
        logger.info("전자세금계산서 국세청 전송상태 처리");
        String mgtKey = (String) data.get("invoicerMgtKey");
        String ntscfnum = (String) data.get("ntsconfirmNum");
        Integer statecode = (Integer) data.get("stateCode");
        String statedt = (String) data.get("stateDT");
        String ntscode = (String) data.get("ntssendErrCode");

        Optional<TB_Salesment> optional = tb_salesmentRepository.findByMgtkeyAndNtscfnum(mgtKey, ntscfnum);

        if (optional.isPresent()) {
            TB_Salesment salesment = optional.get();
            salesment.setStatecode(statecode);
            salesment.setStatedt(statedt);
            salesment.setNtscode(ntscode);

            tb_salesmentRepository.save(salesment);
            logger.info("상태 업데이트 완료");
        } else {
            logger.warn("세금계산서 매칭 실패 - mgtKey: {}, ntsconfirmNum: {}", mgtKey, ntscfnum);
        }

    }

    private void handleReverseRequest(Map<String, Object> data) {
        logger.info("공급받는자 역발행요청 처리");
        // TODO: 구현
    }

    private void handleCancelReverseRequest(Map<String, Object> data) {
        logger.info("공급받는자 역발행요청 취소 처리");
        // TODO: 구현
    }

    private void handleRefuseReverseRequest(Map<String, Object> data) {
        logger.info("공급자 역발행요청 거부 처리");
        // TODO: 구현
    }


}
