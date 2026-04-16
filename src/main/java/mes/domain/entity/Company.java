package mes.domain.entity;

import java.sql.Date;

import javax.persistence.Column;
import javax.persistence.Entity;
import javax.persistence.GeneratedValue;
import javax.persistence.GenerationType;
import javax.persistence.Id;
import javax.persistence.Table;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.NoArgsConstructor;
import org.springframework.web.bind.annotation.RequestParam;

@Entity
@Table(name="company")
@NoArgsConstructor
@Data
@EqualsAndHashCode(callSuper=false)
public class Company extends AbstractAuditModel {
	@Id
	@GeneratedValue(strategy=GenerationType.IDENTITY)
	Integer id;
	
	@Column(name="\"Name\"")
	String name;
	
	@Column(name="\"EngName\"")
	String engName;
	
	@Column(name="\"Code\"")
	String code;
	
	@Column(name="\"Code2\"")
	String code2;
	
	@Column(name="\"Country\"")
	String country;
	
	@Column(name="\"CompanyType\"")
	String companyType;
	
	@Column(name="\"GroupName\"")
	String groupName;
	
	@Column(name="\"BusinessNumber\"")
	String businessNumber;
	
	@Column(name="\"CEOName\"")
	String CEOName;
	
	@Column(name="\"ZipCode\"")
	String zipCode;
	
	@Column(name="\"Address\"")
	String address;

	@Column(name="\"TelNumber\"")
	String telNumber;
	
	@Column(name="\"FaxNumber\"")
	String faxNumber;
	
	@Column(name="\"BusinessType\"")
	String businessType;
	
	@Column(name="\"BusinessItem\"")
	String businessItem;
	
	@Column(name="\"Email\"")
	String email;
	
	@Column(name="\"Homepage\"")
	String homePage;
	
	@Column(name="\"ShippingLocation\"")
	String shippingLocation;
	
	@Column(name="\"Description\"")
	String description;
	
	@Column(name="\"PurchaseSalesDeadline\"")
	String purchaseSalesDeadline;
	
	@Column(name="\"TradingGrade\"")
	String tradingGrade;
	
	@Column(name="\"FirstTradingDay\"")
	Date firstTradingDay;
	
	@Column(name="\"LastTradingDay\"")
	Date lastTradingDay;
	
	@Column(name="\"OurManager\"")
	String ourManager;
	
	@Column(name="\"SalesManager\"")
	String salesManager;
	
	@Column(name="\"AccountManager\"")
	String accountManager;
	
	@Column(name="\"AccountManagerPhone\"")
	String accountManagerPhone;
	
	@Column(name="\"ReceivableAmount\"")
	Float receivableAmount;		//Integer receivableAmount;
	
	@Column(name="\"UnpaidAmount\"")
	Float unpaidAmount;			//Integer unpaidAmount;
	
	@Column(name="\"TrandingBank\"")
	String trandingBank;
	
	@Column(name="\"AccountHolder\"")
	String accountHolder;
	
	@Column(name="\"AccountNumber\"")
	String accountNumber;
	
	@Column(name="\"CreditLimitAmount\"")
	Float creditLimitAmount;	//Integer creditLimitAmount;
	
	@Column(name="\"PaymentCondition\"")
	String paymentCondition;
	
	@Column(name="\"ManageRemark\"")
	String manageRemark;

	@Column(name="relyn")
	String relyn;

	@Column(name="spjangcd")
	String spjangcd;

	@Column(name="\"invoiceEmail\"")
	String invoiceEmail;

	@Column(name="\"IsPersonal\"")
	String isPersonal;

	@Column(name="\"CorporateNumber\"")
	String corporateNumber;

	@Column(name="\"PrintTradingName\"")
	String printTradingName;

}
