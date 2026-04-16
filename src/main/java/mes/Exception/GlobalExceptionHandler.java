package mes.Exception;

import mes.domain.model.AjaxResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log =
            LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(CustomException.class)
    public AjaxResult CustomExceptionHandler(CustomException e){

        log.error("에러 추적:", e);

        AjaxResult r= new AjaxResult();
        r.success = false;
        r.message = e.getMessage();
        return r;
    }
}
