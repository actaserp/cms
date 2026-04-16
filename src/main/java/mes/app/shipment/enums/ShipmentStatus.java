package mes.app.shipment.enums;

public enum ShipmentStatus {

    SHIPPED("shipped"), PARTIAL("partial"), ORDERED("ordered");

    private final String label;


    ShipmentStatus(String label) {
        this.label = label;
    }

    public String getLabel(){
        return label;
    }
}
