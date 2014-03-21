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
    //Data for queue
    ArrayList<Integer> jobIDQueue = new ArrayList<Integer>();           //job in queue
    ArrayList<Integer> taskIDQueue = new ArrayList<Integer>();          //task in queue
    ArrayList<String> classNameQueue = new ArrayList<String>();             //class name in queue
    ArrayList<DataOutputStream> jobStreamQueue = new ArrayList<DataOutputStream>(); //output stream to job in queue

    //Data for running tasks
    ArrayList<Integer> jobIDRunQueue = new ArrayList<Integer>();             //Job ID
    ArrayList<WorkerNode> workerRunQueue = new ArrayList<WorkerNode>();    //Worker for task
    ArrayList<DataInputStream> workerStreamRunQueue = new ArrayList<DataInputStream>(); //Stream between worker and scheduler
    ArrayList<DataOutputStream> jobStreamRunQueue = new ArrayList<DataOutputStream>(); //Stream between job and scheduler

    //variable for indexing into different pools
    int nextJob = 0;

    try{
      //create a ServerSocket listening at specified port - set timeout at 1000ms
      ServerSocket serverSocket = new ServerSocket(schedulerPort);
      serverSocket.setSoTimeout(1000);

      while(true){                                                      //accept connection from worker or client
        //Hold connection info
        Socket socket = null;
        DataInputStream dis = null;
        DataOutputStream dos = null;

        //Check for connections
        try {                                            //accept connection from worker or client
            socket = serverSocket.accept();
            dis = new DataInputStream(socket.getInputStream());
            dos = new DataOutputStream(socket.getOutputStream());
        }catch (SocketTimeoutException exception) {      //if timeout, continue
            socket = null;
            dis = null;
            dos = null;
        }

        //Initialize code
        int code = -1;          
        //If there is a connection, read
        if (dis!=null){    
            code = dis.readInt();
        }
        
        ///////////////////////////Recieve Worker/////////////////////////////////////////
        
        //a connection from worker reporting itself
        if(code == Opcode.new_worker){
          //include the worker into the cluster
          WorkerNode n = cluster.createWorkerNode( dis.readUTF(), dis.readInt());
          if( n == null){          //Creation unsuccessful
            dos.writeInt(Opcode.error);
          }else{                 //Creation successful
            dos.writeInt(Opcode.success);
            dos.writeInt(n.id);
            System.out.println("Worker "+n.id+" "+n.addr+" "+n.port+" created");
          }
                    
          //flush Streams
          dos.flush();
        }

        //////////////////////////Recieve Job//////////////////////////////////////////
        //a connection from client submitting a job
        if(code == Opcode.new_job){
          String className = dis.readUTF();
          long len = dis.readLong();

          //send out the jobId to requester
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
          int numTasks = JobFactory.getJob(fileName, className).getNumTasks();
          
          //Add tasks to queues
          for (int i = 0; i< numTasks;i++){
              jobIDQueue.add(jobId);
              taskIDQueue.add(i);
              classNameQueue.add(className);
              jobStreamQueue.add(dos);
           }
        }else if (socket!=null){         //if socket open, close
            socket.close();  
        }
 
        
        //////////////////////////Handle Queue//////////////////////////////////////////
        while (cluster.checkFreeWorkerNode()==1 && taskIDQueue.size()>0){             //Check for free workers - if there are none, don't block
            //Pull out information about task
            int jobQueue = jobIDQueue.get(nextJob);
            int taskQueue = taskIDQueue.get(nextJob);
            String classQueue = classNameQueue.get(nextJob);
            DataOutputStream streamQueue = jobStreamQueue.get(nextJob);
            
            //get a free worker
            WorkerNode n = cluster.getFreeWorkerNode();
            
            //Remove from queues since free worker found    
            jobIDQueue.remove(nextJob);
            taskIDQueue.remove(nextJob);
            classNameQueue.remove(nextJob);
            jobStreamQueue.remove(nextJob);
  
            //notify the client of job started
            streamQueue.writeInt(Opcode.job_start);
            streamQueue.flush();

            //create connection with worker
            Socket workerSocket = new Socket(n.addr, n.port);
            DataInputStream wis = new DataInputStream(workerSocket.getInputStream());
            DataOutputStream wos = new DataOutputStream(workerSocket.getOutputStream());
            
            //Provide data to worker
            wos.writeInt(Opcode.new_tasks);
            wos.writeInt(jobQueue);
            wos.writeUTF(classQueue);
            wos.writeInt(taskQueue);
            wos.writeInt(1);
            wos.flush();
                            
            //Save data so can get data from worker later
            jobIDRunQueue.add(jobQueue);              //Job ID
            workerRunQueue.add(n);               //Worker
            workerStreamRunQueue .add(wis);       //Stream from worker to scheduler
            jobStreamRunQueue.add(streamQueue);          //Stream from scheduler to job
            
            //Handle Scheduling
            int foundNextFlag = 0;
            //Search for job with a different job ID
            for (int i = nextJob; i<jobIDQueue.size() ;i++){
                if (jobIDQueue.get(i)==jobQueue){
                    continue;
                }else{
                    nextJob = i;
                    foundNextFlag = 1;
                    break;
                }
            }
            //If there were no jobs with a different id, go to beginning of list
            if (foundNextFlag==0){
                nextJob = 0;
            }
        }

        
        //////////////////////////Handle Finished Job//////////////////////////////////////////
        //See if any jobs are finished
        for (int i =0; i<jobIDRunQueue.size(); i++){
            //Pull data about ongoing job
            int jobID = jobIDRunQueue.get(i);
            WorkerNode worker = workerRunQueue.get(i);
            DataInputStream workerStream = workerStreamRunQueue.get(i);
            DataOutputStream jobStream = jobStreamRunQueue.get(i);
            
            //Choose who to schedule
            
            //Check if worker on task has anything to return
            if (workerStream.available()>0){
                //Get information about task
                while(workerStream.readInt() == Opcode.task_finish) {
                    jobStream.writeInt(Opcode.job_print);
                    jobStream.writeUTF("task "+workerStream.readInt()+" finished on worker "+worker.id);
                    jobStream.flush();
                }

                //Remove Job & worker since done
                jobIDRunQueue.remove(i);
                workerRunQueue.remove(i);
                //Remove connections
                workerStreamRunQueue.remove(i);
                jobStreamRunQueue.remove(i);

                // free the worker and free the stream
                workerStream.close();
                cluster.addFreeWorkerNode(worker);

                //notify the client if last task in job
                if (jobIDRunQueue.contains(jobID)==false && jobIDQueue.contains(jobID)==false){
                    jobStream.writeInt(Opcode.job_finish);
                    jobStream.close();
                }
            }
          
         }
      }//keep repeating
      
    } catch(Exception e) {
      e.printStackTrace();
    }
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

    int checkFreeWorkerNode() {
        //if free note avaialble return 1
        if (freeWorkers.size() > 0){
            return 1;
        }else{
            return 0;
        }
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
