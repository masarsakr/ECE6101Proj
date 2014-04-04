java -jar scheduler.jar 51000 &
java -jar worker.jar localhost 51000 51001 &
java -jar worker.jar localhost 51000 51002 &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello >> a.txt &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello >> a.txt &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello >> a.txt &
java -jar client.jar localhost 51000 jobs.jar jobs.Hello  >> a.txt & 