package mes.app.util;

import mes.Encryption.EncryptionKeyProvider;
import mes.Encryption.EncryptionUtil;
import mes.domain.model.AjaxResult;
import org.apache.commons.lang3.StringUtils;

import javax.servlet.http.HttpSession;
import java.io.IOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;
import java.util.List;
import java.util.Optional;

public class UtilClass {

    //안전하게 int 반환 , 기본값 지정 및 캐스팅 실패시 defaultValue로 리턴
    public static Integer toIntOrDefault(Object obj, int defaultValue){

            if(obj instanceof Number){
                return ((Number) obj).intValue();
            }

            if(obj instanceof String){
                try {
                    return Integer.parseInt((String) obj);
                }catch (NumberFormatException e){
                    return defaultValue;
                }
            }
            return defaultValue;
    }


    public static Integer getInt(Map<String, Object> map, String key) {
        if (map == null || key == null) return null;

        Object val = map.get(key);
        if (val instanceof Number) {
            return ((Number) val).intValue();
        }

        return null;
    }

    public static Integer parseInteger(Object obj){
        if(obj == null) return null;
        if (obj instanceof Integer) return (Integer) obj;

        try{
            return Integer.parseInt(obj.toString());
        }catch (NumberFormatException e){
            return null;
        }
    }

    public static boolean isValidDate(String yyymmdd){
        try{
            LocalDate.parse(yyymmdd, DateTimeFormatter.ofPattern("yyyyMMdd"));

            return true;
        }catch(DateTimeParseException e){
            return false;
        }
    }

    /**
     * @Return : yyyyMMddHHmmss
     */
    public static String combineDateAndHourReturnyyyyMMddHHmmss(String date, String time){
        try{

            if(time == null || time.isBlank()){
                time = LocalTime.now().format(DateTimeFormatter.ofPattern("HH:mm"));
            }

            String combined = date + " " + time;

            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm");
            LocalDateTime dateTime = LocalDateTime.parse(combined, formatter);

            DateTimeFormatter output = DateTimeFormatter.ofPattern("yyyyMMddHHmmss");
            return dateTime.format(output);

        }catch (DateTimeParseException e){
            throw new IllegalArgumentException("날짜/시간 형식이 올바르지 않습니다.");
        }
    }

    /**
     * 세션에서 사업장 코드를 통해 사업자 번호를 추출하는 메서드
     * ***/
    public static Map<String, Object> getSpjanInfoFromSession(String spjangcd, HttpSession httpSession){
        List<Map<String, Object>> spjangList = (List<Map<String, Object>>) httpSession.getAttribute("spjangList");

        if(spjangList == null) return null;

        for(Map<String, Object> item : spjangList){
            if(spjangcd.equals(item.get("spjancd"))){
                return item;
            }
        }
        return null;
    }

    /***
     * 세션에서 사업자번호 추출하는 메서드
     * **/
    public static String getsaupnumInfoFromSession(String spjangcd, HttpSession httpSession){
        List<Map<String, Object>> spjangList = (List<Map<String, Object>>) httpSession.getAttribute("spjangList");

        if(spjangList == null) return null;

        for(Map<String, Object> item : spjangList){
            if(spjangcd.equals(item.get("spjangcd"))){
                return String.valueOf(item.get("saupnum"));
            }
        }
        return null;
    }

    /**
     * 객체를 안전하게 문자열로 변환한다.
     * - null일 경우 빈 문자열("") 반환
     * - null이 아니면 toString() 결과 반환
     */
    public static String getStringSafe(Object obj) {
        return obj == null ? "" : obj.toString().trim();
    }

    /**
     * 요소를 순회하며 지정된 컬럼의 암호화된 값들을 복호화한 뒤 일부 자릿수를 마스킹 처리해 덮어씌우는 메서드
     *
     * @param list         암호화된 값을 가진 Map 객체들의 리스트
     * @param col          복호화 및 마스킹할 컬럼 이름
     * @param maskLength   복호화 후 마스킹할 길이 (끝에서 몇 자리 마스킹할지 지정)
     * @throws Exception   복호화 도중 예외 발생 시
     */
    public static void decryptEachItem(List<Map<String, Object>> list, String col, int maskLength) throws IOException {
        byte[] key = EncryptionKeyProvider.getKey();

        for (int i = 0; i < list.size(); i++) {
            Map<String, Object> item = list.get(i);
            Object encrypt = item.get(col);
            String parsedEncrypt = encrypt != null ? encrypt.toString() : "";

            if (!parsedEncrypt.isEmpty()) {
                try {
                    // 복호화 시도
                    String decrypted = EncryptionUtil.decrypt(parsedEncrypt, key);
                    String masked = applyMasking(decrypted, maskLength);
                    item.put(col, masked);
                } catch (Exception e) {
                    // 복호화 불가능한 값이므로 평문으로 간주, 그대로 유지
                }
            }
        }
    }

    public static void decryptItem(Map<String, Object> map, String col, int maskLength) throws IOException {
        byte[] key = EncryptionKeyProvider.getKey();

        Object encryptedValue = map.get(col);
        String parsedEncrypt = encryptedValue != null ? encryptedValue.toString() : "";

        if (!parsedEncrypt.isEmpty()) {
            try {
                // 복호화
                String decrypted = EncryptionUtil.decrypt(parsedEncrypt, key);

                // 마스킹
                String masked = applyMasking(decrypted, maskLength);

                // 결과 반영
                map.put(col, masked);
            } catch (Exception e) {
                // 복호화 불가 → 그대로 유지
            }
        }
    }


    /**
     * 문자열의 끝에서부터 지정된 길이만큼 마스킹 처리하는 메서드
     *
     * @param input       원본 문자열
     * @param maskLength  마스킹할 길이
     * @return 마스킹된 문자열
     */
    private static String applyMasking(String input, int maskLength) {
        if (input == null || input.length() <= maskLength) {
            return "⋆".repeat(Math.max(0, input.length())); // 전체 마스킹
        }
        int visibleLength = input.length() - maskLength;
        return input.substring(0, visibleLength) + "⋆".repeat(maskLength);
    }

}
