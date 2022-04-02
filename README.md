# datalake-case

<!-- vscode-markdown-toc -->
* [Overview](#Overview)
* [Architecture](#Architecture)
* [Running this solution](#Runningthissolution)
* [What is missing](#Whatisunfortunatelymissingduetotimeconstraints)
* [References](#References)

<!-- vscode-markdown-toc-config
	numbering=false
	autoSave=true
	/vscode-markdown-toc-config -->
<!-- /vscode-markdown-toc --> 


This repository contains a solution to a proposed [Data Lake Case](https://github.com/ifood/ifood-data-architect-test).


## <a name='Overview'></a>Overview

The solution presented here is a POC covering aspects of infrastructure, topics on governance, and business-tailored ETLs implementation. **Operationally, the solution proposed here is fully automated and does not require any interaction** --- yet you are more than welcome to monitor and navigate throughout the solution and the resulting data. 


Although the solution may meet the coding requirements of the case, it is extremely important to highlight that it is not the main challenge behind properly deploying a data lake. Counterintuitively, the main challenge regarding data lakes pervades aspects of *Automation*, *Cataloging*, *Serving*, and *Governance* [[1]](#ref1) [[2]](#ref2).
Alternatives for each building block of this data lake solution are proposed in [this supporting presentation](https://docs.google.com/presentation/d/1dqd5nFVog9A7GKW1vDiE47-fo9FQKOgSf7xj9l65wBY/present#slide=id.ga303b7c7f7_0_5) (slide 3). 


Current implementation was designed to be executed on-premises on a single host via Docker due to easier reproducibility sakes. The architectural design/decision process, however, heavily focuses on scaling.
Regarding such scalability, all tools used in this solution can be horizontally scaled (it does not mean that the task complexity is being neglected; instead, primitive bottlenecks were avoided by default). Such scaling can be achieved on-premises via, for instance, container-orchestration services on a cluster or even replaced by cloud-managed services. 


<!-- Further details and points of attention in the architecture are discussed in section [Architecture](#Architecture). -->



## <a name='Architecture'></a>Architecture 

Below, the [original image](https://github.com/ifood/ifood-data-architect-test/blob/master/datalake.png) is adapted to briefly depict the proposed solution.
As shown, the solution covers aspects of automated layer building (Apache NiFi), object storage (MinIO), Apache Spark job API (Apache Livy), query engine (Presto), querying interface (Metabase), etc. More details regarding how the infrastructure was orchestrated can be found in the [docker-compose.yml file](./docker-compose.yml).


<div align="center">
<a href=""> <img style="border: 1px solid #ddd" width="90%" src="https://raw.githubusercontent.com/matheusmota/datalake-case/main/resources/images/arch.png"/> </a>
</div>

<br/>

Summarizing the goals of each service, table below also points to respective documentation and indicates default ports.

**List of services:** 

| Service/tool Name *                                                          | Role/Goal                                 | URL User/Password                             |
|------------------------------------------------------------------------------|-------------------------------------------|-----------------------------------------------|
| <a href="https://nifi.apache.org/" target="_blank">Apache NiFi</a>           | Data flow orchestration                   | [127.0.0.1:8080](http://127.0.0.1:18080/nifi) |
| <a href="https://min.io"           target="_blank">Minio</a>                 | Object storage compatible with Amazon S3  | [127.0.0.1:9000](http://127.0.0.1:9000)  <small>minioadmin/minioadmin</small>|
| <a href="https://bit.ly/2LeREpB"   target="_blank">Metabase</a>              | BI/query UI for Presto                    | [127.0.0.1:3000](http://127.0.0.1:3000)   <small>admin@admin.com/admin123</small>|
| <a href="https://bit.ly/39LBCO2"   target="_blank">Jupyter Docker Stacks</a> | Docker image with Jupyter and Spark **    | [127.0.0.1:8888](http://127.0.0.1:8888)     |
| <a href="https://livy.apache.org/" target="_blank">Apache Livy</a>           | REST-based interface for Spark jobs       | [127.0.0.1:8998](http://127.0.0.1:8998)       |
| <a href="https://prestodb.io/"     target="_blank">Presto</a>                | ANSI SQL query engine                     | [127.0.0.1:18080](http://127.0.0.1:18080)     |
<small>
*   This project also uses other satellite services, such as PostgreSQL/H2 for Metabase; Apache Hadoop and Apache Hive as Presto metastore; Apache Zookeeper for Apache NiFi, etc. Please see the <a href="./docker-compose.yml"> docker-compose.yml </a> file for more details.  <br/>
**  SparkContexts UIs are available at incremental ports starting from 4040 -- e.g., <a href="http://127.0.0.1:4040">http://127.0.0.1:4040</a>, <a href="http://127.0.0.1:4041">http://127.0.0.1:4041</a>,  etc.
</small> 



## <a name='Runningthissolution'></a>Running this solution

### <a name='Quickin-videowalkthrough'></a>Quick in-video walkthrough 
<div align="center">
<a href="https://youtu.be/Vk6tzIwJUW0 "> <img style="border: 1px solid #ddd" width="60%" src="https://raw.githubusercontent.com/matheusmota/datalake-case/main/resources/images/snap.gif"/> </a>

**Video URL:** https://youtu.be/Vk6tzIwJUW0 
</div>


### <a name='Runninglocally'></a>Running locally

The solution is based on [Docker](https://docs.docker.com/get-docker/) and [Docker Compose](https://docs.docker.com/compose/install/) (was built/tested on Linux/Ubuntu environments). In a Linux environment with a proper installation of both tools, the solution can be reproduced via:

```bash
git clone https://github.com/matheusmota/datalake-case.git
cd datalake-case
docker-compose up -d
docker-compose logs -f check-services 
```


The code above will download images, setup the containers and start all services. Lastly, a log stream to an auxiliary container regarding monitoring will indicate when all services are ready. The expected result is similar to:

```
ubuntu@test-env:~/git/datalake-case$ docker-compose logs -f check-services 
Attaching to check-services
check-services           | External IP: 35.173.50.242
check-services           | Waiting for services...
check-services           | Jupyter/Spark is ready!  http://127.0.0.1:8888      or http://35.173.50.242:8888
check-services           | Apache NiFi   is ready!  http://127.0.0.1:8080/nifi or http://35.173.50.242:8080/nifi
check-services           | Minio (S3)    is ready!  http://127.0.0.1:9000      or http://35.173.50.242:9000
check-services           | Metabase      is ready!  http://127.0.0.1:3000      or http://35.173.50.242:3000
check-services           | Apache Livy   is ready!  http://127.0.0.1:8998      or http://35.173.50.242:8998
check-services           | Presto        is ready!  http://127.0.0.1:18080     or http://35.173.50.242:18080
check-services           | All services checked.
check-services exited with code 0
```
<small style="color:red">In a test environment it took ~4min for all services to be ready on a 4vCPU/16gb/100gb-gp2ssd host (AWS EC2 m5.xlarge with no extra customization). This period includes Docker images downloading and all startup procedures. The time will vary proportionately to different resources. </small>




## <a name='Whatisunfortunatelymissingduetotimeconstraints'></a>What is missing

The time available to finish this solution was limited (unfortunately). Therefore, further improvements in the whole data lake architecture were left as future steps -- most important ones are listed below.

#### <a name='InfrastructureandTools'></a>Infrastructure and Tools
1. **Apache Atlas** - As previously mentioned, governance is one of the key challenges surrounding data lakes. Atlas is a governance service that could be used to meet compliance and policies. Furthermore, Atlas provides support to data lineage/provenance. Atlas was considered here as an on-premises alternative to GCP Data Catalog and AWS Glue.
2. **Messaging System** - Production data lakes often involve maintaining a myriad of tools/services. Therefore, providing an unified view of such complex organism is critical. Publishing and consuming messages in a shared, distributed bus is a very powerful way of providing such unified view. For instance, Apache NiFi has plenty of integration processors for Kafka messages exchanging. Such processors could be used for monitoring steps of the pipe and further key business-tailored integration -- e.g., Apache Atlas catalog (re)indexation triggering.
3. **Container Orchestration** - Since it was decided to adopt on-premises services, using an orchestrator like Kubernets would allow easier scaling. 
4. **Apache Ranger** - No security-related initiatives are covered in this solution. Apache Ranger could be used for monitoring and managing data access/security across the lake. Since the scenario has sensitive information, it would be important to implement access policies in higher granularity levels (i.e., columns level).

#### <a name='Implementationfeatures'></a>Feature implementation
1. Use/implement more robust anonymization models instead of only SHA approaches (even using prefixes and suffixes).
2. Improve current job packaging/reuse.
3. Implement a generic tool for metrics validation instead of using tailored solutions on each job.


## <a name='References'></a>References

<a name='ref1'>[[1]](https://www.goodreads.com/book/show/23463279)</a> Kleppmann, Martin. "Designing data-intensive applications" (2015).

<a name='ref2'>[[2]](https://www.goodreads.com/book/show/27560182)</a> Gorelik, Alex. "The enterprise big data lake: Delivering the promise of big data and data science" (2019).
