@echo off
start "Scheduler0" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 1
start "Scheduler1" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 1 1
start "Scheduler2" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 2 1
start "RM0" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 2
start "RM1" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 1 2
start "RM2" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 2 2
start "jobHandler" java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 0
pause

