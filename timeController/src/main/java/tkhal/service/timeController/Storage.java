package tkhal.service.timeController;

public class Storage {
    private static StringBuilder pack = new StringBuilder();

    public static StringBuilder setPack(String message) {
        return pack.append(message + " ");
    }

    public static StringBuilder getPack() {
        return pack;
    }

    public static StringBuilder clear(StringBuilder record) {
        return pack = pack.delete(0, record.length());
    }
}
