# Script template para demonios
#!/bin/sh
 


programa_zookeeper=/home/lramirez/Documents/zookeeper/zookeeper-3.4.6/   #HOME del programa a ejecutar
startup_zookeeper=$programa_zookeeper/bin/zkServer.sh   #código para iniciar el programa
shutdown_zookeeper=$programa_zookeeper/bin/zkServer.sh  #código para parar el programa

programa_storm=/home/lramirez/Documents/storm/apache-storm-0.9.4  #HOME del programa a ejecutar
startup_storm=$programa_storm/bin/storm   #código para iniciar el programa
#shutdown_storm=$programa_storm/bin/storm #código para parar el programa



start(){
 echo -n $"Starting service: "
 $startup_zookeeper start
 RETVAL=$?
 echo

echo -n $"Starting service: "
  nohup $startup_storm nimbus &
 RETVAL=$?
 echo

echo -n $"Starting service: "
 nohup $startup_storm supervisor &
 RETVAL=$?
 echo

echo -n $"Starting service: "
  nohup $startup_storm ui &
 RETVAL=$?
 echo

echo -n $"Starting service: "
nohup $startup_storm logviewer &
 RETVAL=$?
 echo

}
 
stop(){
 echo -n $"Stopping service: "
 $shutdown_zookeeper stop
killall java
 RETVAL=$?
 echo
}
 
restart(){
 stop
 sleep 10
 start
}
 
# Dependiento del parametro que se le pase
#start - stop - restart ejecuta la función correspondiente.
case "$1" in
start)
 start
 ;;
stop)
 stop
 ;;
restart)
 restart
 ;;
*)
 echo $"Usar: $0 {start|stop|restart}"
 exit 1
esac
 
exit 0
