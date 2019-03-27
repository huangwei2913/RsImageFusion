# RsImageFusion
remote sensing image fusion

Please ensure that a Kubernetes cluster is build before running the codes (https://192.168.1.106:6443 in our experiment).The main class is RsImageFusion.java which take five parquet files as input. we build a tool named tiff2parquet.py that can be used to convert tiff images to parquet files. Build a NFS provisioner for containers to mount the images, and Following steps can be used to test the RS image fusion algorigthm. 

step 1:
push the codes into example src directory of Spark code and build the code with Kubernetes supports 
./build/mvn   -pl  :spark-examples_2.11  -T 1C -P kubernetes -DskipTests clean package 

step 2:
push the example jars and relevant libaries to a local docker repository
./bin/docker-image-tool.sh -r 192.168.1.106:5000 -t latest build && \
./bin/docker-image-tool.sh -r 192.168.1.106:5000 -t latest push 

step 3:
submit to the K8s cluster using the following commnad:

./bin/spark-submit --master k8s://https://192.168.1.106:6443 \
--deploy-mode cluster \
--name RsImageFusion \
--class org.apache.spark.examples.RsImageFusion  \
--conf spark.driver.memory=8096m \
--conf spark.executor.memory=10240m \
--conf spark.driver.memoryOverhead=1024m  \
--conf spark.dynamicAllocation.enabled=false \
--conf spark.executor.instances=25  \
--conf spark.kubernetes.memoryOverheadFactor=0.4  \
--conf spark.driver.maxResultSize=3g   \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.kubernetes.container.image=192.168.1.106:5000/spark:latest \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfspvc.mount.path=/mnt  \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfspvc.mount.readOnly=false \
--conf spark.kubernetes.driver.volumes.persistentVolumeClaim.nfspvc.options.claimName=shuffleclaim \
--conf  spark.kubernetes.executor.volumes.persistentVolumeClaim.nfspvc.mount.path=/mnt  \
--conf  spark.kubernetes.executor.volumes.persistentVolumeClaim.nfspvc.mount.readOnly=false \
--conf spark.kubernetes.executor.volumes.persistentVolumeClaim.nfspvc.options.claimName=shuffleclaim \
local:///opt/spark/examples/target/scala-2.11/jars/spark-examples_2.11-2.4.0.jar /mnt/coarse0.parquet 




