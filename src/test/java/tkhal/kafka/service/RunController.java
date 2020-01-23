package tkhal.kafka.service;

public class RunController extends Thread {

    @Override
    public void run() {
        String[] args = new String[0];
        TimeController controller = new TimeController();
        try {
            controller.main(args);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
