/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package mr;

import com.sun.management.OperatingSystemMXBean;
import static java.lang.Thread.sleep;
import java.lang.management.ManagementFactory;


/**
 *
 * @author ASUS
 */
class MeasurementThread extends Thread {
    private final OperatingSystemMXBean operatingSystemMXBean;
    private final Runtime runtime;
    
    private Double totalCPU = 0.0;
    private Double countCPU = 0.0;
    
    private double cpuUsage = 0.0;
            
    private long totalMemory = 0;
    private long count = 0;
    
    private Boolean isRun = true;
    
    public long GetMemoryUsage()
    {
        long total, free, used;

        total = runtime.totalMemory();
        free = runtime.freeMemory();
        used = total - free;
        
        return (used);
    }
    
    public MeasurementThread(String str,Runtime runtime) {
	super(str);
        this.runtime = runtime;
        operatingSystemMXBean = (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
    }
    
    public void run() {
        try {
            do {    
                cpuUsage = (operatingSystemMXBean.getSystemCpuLoad() * 100);
                totalCPU += (cpuUsage > 5.0) ? cpuUsage : 0;
                countCPU += (cpuUsage > 5.0) ? 1 : 0;
                
                totalMemory += GetMemoryUsage();
                
                count++;
                sleep(1000);
            }while(isRun);
        } catch (InterruptedException e) {
            System.out.println("Error : " + e.getMessage());
        }
    }
    
    public void Stop(){
        isRun = !isRun;
    }
    public void PrintAverage(){
        System.out.println("Average CPU Usage\t\t: "+ ((double) totalCPU/countCPU) +" %");
        System.out.println("Average Memory Usage\t\t: "+ ((long) totalMemory/count) +" b");
    }
}
