#!/bin/bash
xterm -T "rmi registry" -e "rmiregistry 5000 -J-Djava.class.path=target/gridscheduler-0.1-jar-with-dependencies.jar" &
xterm -T "GS 0"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 1 > gs0.txt" &
xterm -T "GS 1"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 1 1 > gs1.txt" &
xterm -T "GS 2"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 2 1 > gs2.txt" &
xterm -T "GS 3"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 3 1 > gs3.txt" &
xterm -T "GS 4"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 4 1 > gs4.txt" &
xterm -T "RM 0"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 2 > rm0.txt" & 
xterm -T "RM 1"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 1 2 > rm1.txt" & 
xterm -T "RM 2"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 2 2 > rm2.txt" &
xterm -T "RM 3"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 3 2 > rm3.txt" &
xterm -T "RM 4"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 4 2 > rm4.txt" &
# xterm -T "RM 4"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 4 2 > rm4.txt" &
# xterm -T "RM 5"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 5 2 > rm5.txt" &
# xterm -T "RM 6"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 6 2 > rm6.txt" &
#xterm -T "RM 7"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 7 2" &
#xterm -T "RM 8"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 8 2" &
#xterm -T "RM 9"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 9 2" &
#xterm -T "RM 10"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 10 2" & 
#xterm -T "RM 11"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 11 2" &
#xterm -T "RM 12"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 12 2" &
#xterm -T "RM 13"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 13 2" &
#xterm -T "RM 14"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 14 2" &
#xterm -T "RM 15"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 15 2" &
#xterm -T "RM 16"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 16 2" &
#xterm -T "RM 17"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 17 2" &
#xterm -T "RM 18"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 18 2" &
#xterm -T "RM 19"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 19 2" &
xterm -T "Simulator"  -e "java -jar target/gridscheduler-0.1-jar-with-dependencies.jar false 0 0 anon_jobs.gwf" &

