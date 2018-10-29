import com.amazonaws.services.datapipeline.DataPipeline
import com.amazonaws.services.datapipeline.DataPipelineClientBuilder
import com.amazonaws.services.datapipeline.model.ActivatePipelineRequest
import com.amazonaws.services.datapipeline.model.ActivatePipelineResult
import com.amazonaws.services.datapipeline.model.CreatePipelineRequest
import com.amazonaws.services.datapipeline.model.CreatePipelineResult
import com.amazonaws.services.datapipeline.model.Field
import com.amazonaws.services.datapipeline.model.PipelineObject
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionRequest
import com.amazonaws.services.datapipeline.model.PutPipelineDefinitionResult

/**
 * A simple program to create a AWS Data Pipeline in Java.
 */
class Main {

    static void main(String [] args) {
        DataPipeline dataPipeline = createDataPipeline()
        String pipelineId = createPipelineRequest(dataPipeline)
        createPipelineObjects(dataPipeline, pipelineId)
    }

    static String createPipelineRequest(DataPipeline dataPipeline) {
        CreatePipelineRequest createPipeline = new CreatePipelineRequest().withName("RDSToS3").withUniqueId("UniqueId")
        CreatePipelineResult createPipelineResult = dataPipeline.createPipeline(createPipeline)
        return createPipelineResult.getPipelineId()
    }

    static PipelineObject createConfigurationObject() {
        PipelineObject defaultObject = new PipelineObject()
                .withName("Default")
                .withId("Default")
                .withFields(
                new Field().withKey("failureAndRerunMode").withStringValue("cascade"),
                new Field().withKey("schedule").withRefValue("DefaultSchedule"),
                // a IAM role called DataPipelineDefaultResourceRole was created and AmazonEC2RoleforDataPipelineRole
                new Field().withKey("resourceRole").withStringValue("DataPipelineDefaultResourceRole"),
                // a IAM role called data_pipeline was created with AWSDataPipelineRole
                new Field().withKey("role").withStringValue("data_pipeline"),
                // bucket in s3 called data-pipeline-project-stephen-demo was already created
                new Field().withKey("pipelineLogUri").withStringValue("s3://data-pipeline-project-stephen-demo/logs/"),
                new Field().withKey("scheduleType").withStringValue("cron")
        )
        return defaultObject
    }

    static PipelineObject createScheduleObject() {
        PipelineObject schedule = new PipelineObject()
                .withName("DefaultSchedule")
                .withId("DefaultSchedule")
                .withFields(
                new Field().withKey("type").withStringValue("Schedule"),
                new Field().withKey("period").withStringValue("1 Day"),
                new Field().withKey("startAt").withStringValue("FIRST_ACTIVATION_DATE_TIME")
        )
        return schedule
    }

    static PipelineObject createRdsDatabase() {
        PipelineObject rdsDatabase = new PipelineObject()
                .withName("rds_mysql")
                .withId("rds_mysql")
                .withFields(
                new Field().withKey("type").withStringValue("RdsDatabase"),
                // rds instance with password "password" was already created
                new Field().withKey("*password").withStringValue("password"),
                // rds instance with name db_instance was already created
                new Field().withKey("rdsInstanceId").withStringValue("db_instance"),
                // rds instance with username "username" was already created
                new Field().withKey("username").withStringValue("username"),
                // rds instance with database name "db" was already created
                new Field().withKey("databaseName").withStringValue("db")
        )
        return rdsDatabase
    }

    static PipelineObject createSourceRDSTable() {
        PipelineObject sourceRDSTable = new PipelineObject()
                .withName("SourceRDSTable")
                .withId("SourceRDSTable")
                .withFields(
                new Field().withKey("type").withStringValue("SqlDataNode"),
                new Field().withKey("database").withRefValue("rds_mysql"),
                // rds instance with table name "db_table" was already created
                new Field().withKey("table").withStringValue("db_table"),
                new Field().withKey("selectQuery").withStringValue("select * from db_table LIMIT 1")
        )
        return sourceRDSTable
    }

    static PipelineObject createEc2Instance() {
        PipelineObject ec2Resource = new PipelineObject()
                .withName("Ec2Instance")
                .withId("Ec2Instance")
                .withFields(
                new Field().withKey("type").withStringValue("Ec2Resource"),
                new Field().withKey("instanceType").withStringValue("t2.micro"),
                new Field().withKey("actionOnTaskFailure").withStringValue("terminate"),
                new Field().withKey("terminateAfter").withStringValue("2 Hours"),
                // IAM role data_pipeline was already created
                new Field().withKey("role").withStringValue("data_pipeline"),
                // A created AMI from an ec2 instance previously created
                new Field().withKey("imageId").withStringValue("ami-02dea82489a53498b"),
                new Field().withKey("runAsUser").withStringValue("ec2-user")
        )
        return ec2Resource
    }

    static PipelineObject createCopyActivity() {
        PipelineObject copyActivity = new PipelineObject()
                .withName("RDStoS3CopyActivity")
                .withId("RDStoS3CopyActivity")
                .withFields(
                new Field().withKey("type").withStringValue("CopyActivity"),
                new Field().withKey("schedule").withRefValue("DefaultSchedule"),
                new Field().withKey("output").withRefValue("S3OutputLocation"),
                new Field().withKey("input").withRefValue("SourceRDSTable"),
                new Field().withKey("runsOn").withRefValue("Ec2Instance")
        )
        return copyActivity
    }

    static PipelineObject createS3Node() {
        PipelineObject s3DataNode = new PipelineObject()
                .withName("S3OutputLocation")
                .withId("S3OutputLocation")
                .withFields(
                new Field().withKey("type").withStringValue("S3DataNode"),
                // bucket in s3 called data-pipeline-project-stephen-demo was already created
                new Field().withKey("directoryPath").withStringValue("s3://data-pipeline-project-stephen-demo/rds")
        )
        return s3DataNode
    }

    static DataPipeline createPipelineObjects(DataPipeline dataPipeline, String pipelineId) {
        List<PipelineObject> pipelineObjects = new ArrayList<PipelineObject>()

        pipelineObjects.add(createRdsDatabase())
        pipelineObjects.add(createSourceRDSTable())
        pipelineObjects.add(createConfigurationObject())
        pipelineObjects.add(createEc2Instance())
        pipelineObjects.add(createCopyActivity())
        pipelineObjects.add(createS3Node())
        pipelineObjects.add(createScheduleObject())
        PutPipelineDefinitionRequest putPipelineDefintion = new PutPipelineDefinitionRequest()
                .withPipelineId(pipelineId)
                .withPipelineObjects(pipelineObjects)

        PutPipelineDefinitionResult putPipelineResult = dataPipeline.putPipelineDefinition(putPipelineDefintion)
        // tells you if there are any errors when creating the pipeline
        System.out.println(putPipelineResult)

        ActivatePipelineRequest activatePipelineReq = new ActivatePipelineRequest()
                .withPipelineId(pipelineId)
        ActivatePipelineResult activatePipelineResult = dataPipeline.activatePipeline(activatePipelineReq)

        // activatePipelineResults usually returns an empty object {} when it is finished. If we get to this point, we know
        // we can check our AWS Data Pipeline and see if the pipeline is indeed active.
        System.out.println(activatePipelineResult)
        System.out.println(pipelineId)
    }

    static DataPipeline createDataPipeline() {
        return DataPipelineClientBuilder.standard().withRegion("us-west-2").build()
    }
}
