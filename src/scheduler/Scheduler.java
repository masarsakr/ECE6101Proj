package scheduler;

import java.io.*;
import java.net.*;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Date;

import common.*;

public class Scheduler {
    
    //Node for tasks that are running
    class RunningTaskNode{
        public int jobIDRunQueue;                  //Job ID
        public int taskIDRunQueue;                 //Task ID
        public WorkerNode workerRunQueue;          //Worker for task
        public DataInputStream workerStreamRunQueue; //Stream between worker and scheduler
        public DataOutputStream jobStreamRunQueue; //Stream between job and scheduler
        public String classNameRunQueue;           //class name in queue
        public long lastHeartbeatRunQueue;         //Stream between job and scheduler
        
        RunningTaskNode(int jobIDRunQueue, int taskIDRunQueue, WorkerNode workerRunQueue, DataInputStream workerStreamRunQueue, DataOutputStream jobStreamRunQueue, String classNameRunQueue, long lastHeartbeatRunQueue){
            this.jobIDRunQueue = jobIDRunQueue;
            this.taskIDRunQueue = taskIDRunQueue;
            this.workerRunQueue = workerRunQueue;          
            this.workerStreamRunQueue = workerStreamRunQueue; 
            this.jobStreamRunQueue = jobStreamRunQueue; 
            this.classNameRunQueue = classNameRunQueue;           
            this.lastHeartbeatRunQueue = lastHeartbeatRunQueue;      

        }
    }
    
    //Node for tasks waiting to run
    class QueueTaskNode{
        public int jobIDQueue;           //job in queue
        public int taskIDQueue;          //task in queue
        public String classNameQueue;             //class name in queue
        public DataOutputStream jobStreamQueue; //output stream to job in queue
        
        QueueTaskNode(int jobIDQueue, int taskIDQueue, String classNameQueue, DataOutputStream jobStreamQueue){
            this.jobIDQueue = jobIDQueue;
            this.taskIDQueue = taskIDQueue;
            this.classNameQueue = classNameQueue;
            this.jobStreamQueue = jobStreamQueue; 

        }
    }

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
    ArrayList<QueueTaskNode> queueTask = new ArrayList<QueueTaskNode>();

    //Data for running tasks
    ArrayList<RunningTaskNode> runningTask = new ArrayList<RunningTaskNode>();

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
        ////////////////////////////////////////////////////////////////////////////////        
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
        ////////////////////////////////////////////////////////////////////////////////
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
              QueueTaskNode newTask =  new QueueTaskNode(jobId, i, className, dos);
              queueTask.add(newTask);
           }
           
        }else if (socket!=null){         //if socket open, close
            socket.close();  
        }
 
        ////////////////////////////////////////////////////////////////////////////////
        //////////////////////////Handle Queue//////////////////////////////////////////
        while (cluster.checkFreeWorkerNode()==1 && queueTask.size()>0){             //Check for free workers - if there are none, don't block
            //Pull out information about task
            int jobQueue = queueTask.get(nextJob).jobIDQueue;
            int taskQueue = queueTask.get(nextJob).taskIDQueue;
            String classQueue = queueTask.get(nextJob).classNameQueue;
            DataOutputStream streamQueue = queueTask.get(nextJob).jobStreamQueue;
            
            //get a free worker
            WorkerNode n = cluster.getFreeWorkerNode();
  
            //notify the client of job started
            streamQueue.writeInt(Opcode.job_start);
            streamQueue.flush();

            //create connection with worker
            Socket workerSocket;
            DataInputStream wis;
            DataOutputStream wos;

            //If connection fails due to worker dropping
            try{
                //create connection with worker
                workerSocket = new Socket(n.addr, n.port);
                wis = new DataInputStream(workerSocket.getInputStream());
                wos = new DataOutputStream(workerSocket.getOutputStream());
                
                //Provide data to worker
                wos.writeInt(Opcode.new_tasks);
                wos.writeInt(jobQueue);
                wos.writeUTF(classQueue);
                wos.writeInt(taskQueue);
                wos.writeInt(1);
                wos.flush();
            }catch(ConnectException ce){
                //continue to next
                continue;
            }
            
            //Remove from queues since free worker found    
            queueTask.remove(nextJob);
                
            //Save data so can get data from worker later
            RunningTaskNode newTask = new RunningTaskNode(jobQueue, taskQueue, n, wis, streamQueue, classQueue, System.currentTimeMillis());
            runningTask.add(newTask);
            
            ///////////////////////Handle Scheduling/////////////////////////////////////
            int foundNextFlag = 0;
            //Search for job with a different job ID
            for (int i = nextJob; i<queueTask.size() ;i++){
                if (queueTask.get(i).jobIDQueue==jobQueue){
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
            /////////////////////////////////////////////////////////////////////////////
        }
        
        ////////////////////////////////////////////////////////////////////////////////
        //////////////////////////Handle Finished Job//////////////////////////////////////////
        //See if any jobs are finished
        for (int i =0; i<runningTask.size(); i++){
            //Pull data about ongoing job
            int jobID = runningTask.get(i).jobIDRunQueue;
            WorkerNode worker = runningTask.get(i).workerRunQueue;
            DataInputStream workerStream = runningTask.get(i).workerStreamRunQueue;
            DataOutputStream jobStream = runningTask.get(i).jobStreamRunQueue;
            
            //if worker fails, catch and conitnue
            try{
                                                                                                            
                //Check if worker on task has anything to return
                if (workerStream.available()>0){
                    //Read value from worker
                    int valueRead = workerStream.readInt() ; //Fix
                    
                    //Look for heartbeat
                    if (valueRead == Opcode.worker_heartbeat){
                        //If there are mutliple heartbeats in the stream, read them all
                        while(valueRead ==  Opcode.worker_heartbeat) {
                            valueRead = workerStream.readInt();     //Fix
                        }
                        RunningTaskNode modifiedTaskNode = runningTask.get(i);
                        modifiedTaskNode.lastHeartbeatRunQueue = System.currentTimeMillis();
                        runningTask.set(i, modifiedTaskNode);
                    }
                    
                    //Check if job finished
                    if (valueRead == Opcode.task_finish){
                        //Get information about task
                        while(valueRead == Opcode.task_finish) {
                            jobStream.writeInt(Opcode.job_print);
                            jobStream.writeUTF("task "+workerStream.readInt()+" finished on worker "+worker.id);
                            jobStream.flush();
                            valueRead = workerStream.readInt();     //Fix
                        }

                        //Remove Job & worker since done
                        runningTask.remove(i);

                        // free the worker and free the stream
                        workerStream.close();
                        cluster.addFreeWorkerNode(worker);

                        //Check if anything left for job
                        int doneFlag = 1;
                        for (int j = 0; j<runningTask.size() ;j++){
                            if (runningTask.get(j).jobIDRunQueue==jobID){
                                doneFlag = 0; 
                                break; 
                            }
                        }
                        for (int j = 0; j<queueTask.size() && doneFlag==1 ;j++){
                            if (queueTask.get(j).jobIDQueue==jobID){
                                doneFlag = 0; 
                                break; 
                            }
                        }
                        //If not queue has the jobID, then it is finished
                        if (doneFlag==1){
                            jobStream.writeInt(Opcode.job_finish);
                            jobStream.close();
                        }
                    }
                }
                
            }catch(EOFException eof){
                //Close connection with worker
                workerStream.close();

                //Add to wait queue
                QueueTaskNode newTask = new QueueTaskNode(runningTask.get(i).jobIDRunQueue, runningTask.get(i).taskIDRunQueue, runningTask.get(i).classNameRunQueue, runningTask.get(i).jobStreamRunQueue);
                queueTask.add(newTask);

                //Remove from run queue
                runningTask.remove(i); 
                
                continue;
            }

            
            //Check if hearbeat has been dead
            if (runningTask.size()>i && (System.currentTimeMillis()-runningTask.get(i).lastHeartbeatRunQueue)>5000){
                //Close connection with worker
                workerStream.close();

                //Add to wait queue
                QueueTaskNode newTask = new QueueTaskNode(runningTask.get(i).jobIDRunQueue, runningTask.get(i).taskIDRunQueue, runningTask.get(i).classNameRunQueue, runningTask.get(i).jobStreamRunQueue);
                queueTask.add(newTask);

                //Remove from run queue
                runningTask.remove(i); 
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
