# Quick Start

## Install etcd CRD and hugegraph-computer CRD

```bash
kubectl apply -f https://raw.githubusercontent.com/hugegraph/hugegraph-computer/master/computer-k8s-operator/manifest/etcd-operator-crd.v1beta1.yaml

kubectl apply -f https://raw.githubusercontent.com/hugegraph/hugegraph-computer/master/computer-k8s-operator/manifest/hugegraph-computer-crd.v1beta1.yaml
```

## Show CRD

```bash
kubectl get crd

NAME                                        CREATED AT
etcdclusters.etcd.database.coreos.com       2021-09-16T08:00:50Z
hugegraphcomputerjobs.hugegraph.baidu.com   2021-09-16T08:01:08Z
```

## Install etcd-operator and hugegraph-computer-operator

```bash
kubectl apply -f https://raw.githubusercontent.com/hugegraph/hugegraph-computer/master/computer-k8s-operator/manifest/hugegraph-computer-operator.yaml
```

## Wait for operator deployment to complete

```bash
kubectl get pod -n hugegraph-computer-operator-system

NAME                                                              READY   STATUS    RESTARTS   AGE
hugegraph-computer-operator-controller-manager-58c5545949-jqvzl   1/1     Running   0          15h
hugegraph-computer-operator-etcd-28lm67jxk5                       1/1     Running   0          15h
hugegraph-computer-operator-etcd-d42dwrq4ht                       1/1     Running   0          15h
hugegraph-computer-operator-etcd-mpcbt5kh2m                       1/1     Running   0          15h
hugegraph-computer-operator-etcd-operator-5597f97b4d-lxs98        1/1     Running   0          15h
```

## Submit job

```yaml
cat <<EOF | kubectl apply --filename -
apiVersion: hugegraph.baidu.com/v1
kind: HugeGraphComputerJob
metadata:
  namespace: hugegraph-computer-system
  name: &jobName pagerank-sample
spec:
  jobId: *jobName
  algorithmName: page_rank
  image: hugegraph/hugegraph-builtin-algorithms:latest # algorithm image url
  jarFile: /opt/jars/hugegraph-builtin-algorithms.jar
  pullPolicy: Always
  workerCpu: "4"
  workerMemory: "4Gi"
  workerInstances: 5
  computerConf:
    job.partitions_count: "20"
    algorithm.params_class: com.baidu.hugegraph.computer.algorithm.centrality.pagerank.PageRankParams
    hugegraph.url: http://${hugegraph-server-host}:${hugegraph-server-port} # hugegraph server url
    hugegraph.name: hugegraph
EOF
```

## Show job

```bash
kubectl get hcjob/pagerank-sample -n hugegraph-computer-system

NAME               JOBID              JOBSTATUS
pagerank-sample    pagerank-sample    RUNNING
```

## Show log of running nodes

```bash
# Show the master log
kubectl logs -l component=pagerank-sample-master -n hugegraph-computer-system

# Show the worker log
kubectl logs -l component=pagerank-sample-worker -n hugegraph-computer-system
```

## Show diagnostic log of a job

> NOTE: diagnostic log exist only when the job fails, and it will only be saved for one hour.

```bash
kubectl get event --field-selector reason=ComputerJobFailed --field-selector involvedObject.name=pagerank-sample -n hugegraph-computer-system
```

## Show success event of a job

> NOTE: it will only be saved for one hour

```bash
kubectl get event --field-selector reason=ComputerJobSucceed --field-selector involvedObject.name=pagerank-sample -n hugegraph-computer-system
```
