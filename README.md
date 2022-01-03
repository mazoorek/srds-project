# srds-project
1) create cassandra cluster:
kubectl apply -f srds-cassandra.yaml

2) check cluster state:
kubectl get statefulset srds-cassandra
kubectl get pods -l="app=srds-cassandra"
kubectl exec -it cassandra-0 -- nodetool status   
   
3)
kubectl run -it cqlsh --image cassandra:3.11 -- /bin/bash
cqlsh srds-cassandra-0.srds-cassandra

4)
kubectl port-forward service/srds-cassandra 9042:9042

3) delete resources:
grace=$(kubectl get pod srds-cassandra-0 -o=jsonpath='{.spec.terminationGracePeriodSeconds}') \
&& kubectl delete statefulset -l app=srds-cassandra \
&& echo "Sleeping ${grace} seconds" 1>&2 \
&& sleep $grace \
&& kubectl delete persistentvolumeclaim -l app=srds-cassandra
