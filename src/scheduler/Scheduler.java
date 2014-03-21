package scheduler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;

import common.*;

public class Scheduler {

  int schedulerPort;
  Cluster cluster;
  int jobIdNext;

  Scheduler(int p) {
    schedulerPort = p;
    cluster = new Cluster();
    jobIdNext = 1;
  }

  public static void main(String[] args) {
    Scheduler scheduler = new Scheduler(Integer.parseInt(args[0]));
    scheduler.run();
  }

  public void run() {
    ArrayList<WorkerNode> tasksWorker = new ArrayList<WorkerNode>(); //all the workers
    ArrayList<DataInputStream> tasksWorkerStream = new ArrayList<DataInputStream>(); //all the workers
    ArrayList<DataOutputStream> tasksJobStream = new ArrayList<DataOutputStream>(); //all the workers

    try{
      //create a ServerSocket listening at specified port - set timeout at 1000ms
      ServerSocket serverSocket = new ServerSocket(schedulerPort);
      serverSocket.setSoTimeout(1000);

      while(true){                                                      //accept connection from worker or client
        //Hold connection info
        Socket socket = null;
        DataInputStream dis = null;
        DataOutputStream dos = null;

        try {            //accept connection from worker or client
            socket = serverSocket.accept();
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());
        }catch (SocketTimeoutException exception) {      //if timeout, continue
            socket = null;
            dis = null;
            dos = null;
        }

        int code = -1;          //Initialize code
        //If there is a connection, read
        if (dis!=null){    
            code = dis.readInt();
        }
        
        //a connection from worker reporting itself
        if(code == Opcode.new_worker){
          //include the worker into the cluster
          WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
          if( n == null){          //Creation unsuccessful
            dos.writeInt(Opcode.error);
          }
          else{          //Creation successful
            dos.writeInt(Opcode.success);
            dos.writeInt(n.id);
            System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
          }
          dos.flush();
        }

        //a connection from client submitting a job
        if(code == Opcode.new_job){
          String className = dis.readUTF();
          long len = dis.readLong();

          //send out the jobId
          int jobId = jobIdNext++;
          dos.writeInt(jobId);
          dos.flush();

          //receive the job file and store it to the shared filesystem
          String fileName = new String("fs/."+jobId+".jar");
          FileOutputStream fos = new FileOutputStream(fileName);
          int count;
          byte[] buf = new byte[65536];
          while(len > 0) {
            count = dis.read(buf);
            if(count > 0){
              fos.write(buf, 0, count);
              len -= count;
            }
          }
          fos.flush();
          fos.close();
          

          //get the tasks
          int taskIdStart = 0;
        
          int numTasks = JobFactory.getJob(fileName, className).getNumTasks();

         for (int i = 0; i< numTasks;i++){
          //get a free worker
          WorkerNode n = cluster.getFreeWorkerNode();
          
          //notify the client
          dos.writeInt(Opcode.job_start);
          dos.flush();

          //assign the tasks to the worker
          Socket workerSocket = new Socket(n.addr, n.port);
          DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
          DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
          
          wos.writeInt(Opcode.new_tasks);
          wos.writeInt(jobId);
          wos.writeUTF(className);
          wos.writeInt(taskIdStart+i);
          wos.writeInt(1);
          wos.flush();
          //wos.close();
          
          tasksWorker.add(n);
          tasksWorkerStream.add(wis);
          tasksJobStream.add(dos);
          }

        } //go through all wis and try to readInt

        for (int i =0; i<tasksWorker.size(); i++){
            WorkerNode worker = tasksWorker.get(i);
            DataInputStream workerStream = tasksWorkerStream.get(i);
            DataOutputStream jobStream = tasksJobStream.get(i);
          //repeatedly process the worker's feedback
            if (workerStream.available()>0){
           while(workerStream.readInt() == Opcode.task_finish) {
            jobStream.writeInt(Opcode.job_print);
            jobStream.writeUTF("task "+workerStream.readInt()+" finished on worker "+worker.id);
            jobStream.flush();
          }

          //disconnect and free the worker
          //wis.close();
          //workerSocket.close();
          cluster.addFreeWorkerNode(worker);

          //notify the client
          jobStream.writeInt(Opcode.job_finish);
          jobStream.flush();
          }
        }
        
        //If socket connection, close connections
        if (socket!=null){
            //dis.close();
            //dos.close();
            //socket.close();
        }
        }
    } catch(Exception e) {
      e.printStackTrace();
    }
      
    //serverSocket.close();
  }
  

  //the data structure for a cluster of worker nodes
  class Cluster {
    ArrayList<WorkerNode> workers; //all the workers
    LinkedList<WorkerNode> freeWorkers; //the free workers
    
    Cluster() {
      workers = new ArrayList<WorkerNode>();
      freeWorkers = new LinkedList<WorkerNode>();
    }

    WorkerNode createWorkerNode(String addr, int port) {
      WorkerNode n = null;

      synchronized(workers) {
        n = new WorkerNode(workers.size(), addr, port);
        workers.add(n);
      }
      addFreeWorkerNode(n);

      return n;
    }

    WorkerNode getFreeWorkerNode() {
      WorkerNode n = null;

      try{
        synchronized(freeWorkers) {
          while(freeWorkers.size() == 0) {
            freeWorkers.wait();
          }
          n = freeWorkers.remove();
        }
        n.status = 2;
      } catch(Exception e) {
        e.printStackTrace();
      }

      return n;
    }

    void addFreeWorkerNode(WorkerNode n) {
      n.status = 1;
      synchronized(freeWorkers) {
        freeWorkers.add(n);
        freeWorkers.notifyAll();
      }
    }
  }

  //the data structure of a worker node
  class WorkerNode {
    int id;
    String addr;
    int port;
    int status; //WorkerNode status: 0-sleep, 1-free, 2-busy, 4-failed

    WorkerNode(int i, String a, int p) {
      id = i;
      addr = a;
      port = p;
      status = 0;
    }
  }


}
