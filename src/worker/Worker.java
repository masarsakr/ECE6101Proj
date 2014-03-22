package worker;

import java.io.*;
import java.net.*;

import common.*;

class WorkerHeartbeat implements Runnable {
    int taskId;
    DataOutputStream schedulerConnection;
    
    WorkerHeartbeat(int taskId, DataOutputStream schedulerConnection){
        this.taskId = taskId;
        this.schedulerConnection = schedulerConnection;         //connection to scheduler
    }
    
    public void run() {

        try{
        
            //Initial time entered
            long prevTime = System.currentTimeMillis();
            schedulerConnection.writeInt(Opcode.worker_heartbeat);

            //While still processing
            while(!Thread.currentThread().isInterrupted()){

                    //If its been a second since last heartbeat
                    if ((System.currentTimeMillis()-prevTime)>1000){
                            //Send new heartbeat
                            prevTime = System.currentTimeMillis();
                            schedulerConnection.writeInt(Opcode.worker_heartbeat);
                    }
            }
            
            //Send heartbeat
            prevTime = System.currentTimeMillis();
            schedulerConnection.writeInt(Opcode.worker_heartbeat);

            //report to scheduler once a task is finished
            schedulerConnection.writeInt(Opcode.task_finish);
            schedulerConnection.writeInt(taskId);
            schedulerConnection.flush();
        
        } catch(Exception e) {
            e.printStackTrace();
        }

    }
    
    public void terminate(){
        
    }


}
public class Worker {

  String schedulerAddr;
  int schedulerPort;
  String workerAddr;
  int workerPort;

  public static void main(String[] args) {
    Worker worker = new Worker( args[0], Integer.parseInt(args[1]), Integer.parseInt(args[2]));
    worker.run();
  }

  Worker(String sAddr, int sPort, int wPort) {
    schedulerAddr = sAddr;
    schedulerPort = sPort;
    workerAddr = "localhost";
    workerPort = wPort;
  }

  public void run() {
    ServerSocket serverSocket;
    Socket socket;
    DataInputStream dis;
    DataOutputStream dos;
    int workerId;

    try{
      //create a ServerSocket listening at the specified port
      serverSocket = new ServerSocket(workerPort);

      //connect to scheduler
      socket = new Socket(schedulerAddr, schedulerPort);
      dis = new DataInputStream(socket.getInputStream());
      dos = new DataOutputStream(socket.getOutputStream());
   
      //report itself to the scheduler
      dos.writeInt(Opcode.new_worker);
      dos.writeUTF(workerAddr);
      dos.writeInt(workerPort);
      dos.flush();

      if(dis.readInt() == Opcode.error){
        throw new Exception("scheduler error");
      }
      workerId = dis.readInt();

      dis.close();
      dos.close();
      socket.close();

      System.out.println("This is worker "+workerId);
      
      //repeatedly process scheduler's task assignment
      while(true) {
        //accept an connection from scheduler
        socket = serverSocket.accept();
        dis = new DataInputStream(socket.getInputStream());
        dos = new DataOutputStream(socket.getOutputStream());

        if(dis.readInt() != Opcode.new_tasks)
          throw new Exception("worker error");

        //get jobId, fileName, className, and the assigned task ids
        int jobId = dis.readInt();
        String fileName = new String("fs/."+jobId+".jar");
        String className = dis.readUTF();
        int taskIdStart = dis.readInt();
        int numTasks = dis.readInt();

        //read the job file from shared file system
        Job job = JobFactory.getJob(fileName, className);

        //Flag to hold whether job being worked on
        int working = 0;

        //execute the assigned tasks
        for(int taskId=taskIdStart; taskId<taskIdStart+numTasks; taskId++){          
          //Thread to send heartbeat
          Thread t1 = new Thread(new WorkerHeartbeat(taskId, dos));
          t1.start();
          
          //Perform task
          job.task(taskId);
          
          //Tell hearbeat thread done and end
          t1.interrupt();
          t1.join();
        }

        //disconnect
        dos.writeInt(Opcode.worker_finish);
        dos.flush();
        dis.close();
        dos.close();
        socket.close();
      }
    } catch(Exception e) {
      e.printStackTrace();
    }
  }

}
