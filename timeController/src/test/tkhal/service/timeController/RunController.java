package tkhal.service.timeController;

public class RunController extends Thread {

    @Override
    public void run() {
        String[] args = new String[0];
        TimeController controller = new TimeController();
        controller.main(args);
    }
}
