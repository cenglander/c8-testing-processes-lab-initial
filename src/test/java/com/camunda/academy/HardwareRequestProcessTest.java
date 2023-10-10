package com.camunda.academy;

import io.camunda.zeebe.client.ZeebeClient;
import io.camunda.zeebe.client.api.response.ActivateJobsResponse;
import io.camunda.zeebe.client.api.response.ActivatedJob;
import io.camunda.zeebe.client.api.response.DeploymentEvent;
import io.camunda.zeebe.client.api.response.ProcessInstanceEvent;
import io.camunda.zeebe.model.bpmn.Bpmn;
import io.camunda.zeebe.process.test.api.ZeebeTestEngine;
import io.camunda.zeebe.process.test.assertions.BpmnAssert;
import io.camunda.zeebe.process.test.assertions.DeploymentAssert;
import io.camunda.zeebe.process.test.extension.ZeebeProcessTest;
import io.camunda.zeebe.process.test.filters.RecordStream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.concurrent.impl.FutureConvertersImpl;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import static org.assertj.core.api.Assertions.fail;

@ZeebeProcessTest
public class HardwareRequestProcessTest {

    private ZeebeTestEngine engine;
    private ZeebeClient client;
    //private RecordStream recordStream;

    @BeforeEach
    public void setup() {
        DeploymentEvent deploymentEvent = client.newDeployResourceCommand()
                .addResourceFromClasspath("hardwarerequest.bpmn")
                .send().join();
        DeploymentAssert assertions = BpmnAssert.assertThat(deploymentEvent)
                .containsProcessesByBpmnProcessId("HardwareRequestProcess");
    }

    @Test
    public void testHappyPath() {

        //given
        int PRICE = 500;
        Map startVars = Map.of("price", PRICE);

        //when
        ProcessInstanceEvent processInstance = startInstance("HardwareRequestProcess", startVars);
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("ServiceTask_CheckAvailability");

        completeJob("check-availability", 1, Map.of("available", true));

        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("ServiceTask_SendHardware");

        completeJob("send-hardware", 1, Map.of());

        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("EndEvent_HardwareSent")
                .isCompleted();

    }
    @Test
    public void testHardWareNotAvailable() {

        //given
        String ORDER_ID = "test";
        Map startVars = Map.of("available", false, "orderId", ORDER_ID);

        //when
        ProcessInstanceEvent processInstance = startInstanceBefore("HardwareRequestProcess", startVars, "Gateway_HardwareAvailable");
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("ServiceTask_OrderHardware");
        completeJob("order-hardware", 1, Map.of());

        client.newPublishMessageCommand()
                .messageName("hardwareReceived")
                .correlationKey("test")
                .send().join();
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("ServiceTask_SendHardware");

       /* completeJob("send-hardware", 1, Map.of());
        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("ServiceTask_SendHardware")
                .hasPassedElement("EndEvent_HardwareSent")
                .isCompleted();*/
    }

    @Test
    public void testSupplierNotDeliveringHardwareInTime() throws InterruptedException, TimeoutException {
        //given
        String ORDER_ID = "test";
        Map startVars = Map.of("orderId", ORDER_ID);

        //when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("HardwareRequestProcess", startVars, "Gateway_WaitForHardware");
        engine.increaseTime(Duration.ofDays(7));
        engine.waitForBusyState(Duration.ofSeconds(1));
        engine.waitForIdleState(Duration.ofSeconds(1));

        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("UserTask_CallWithSupplier");

        completeUserTask(1, Map.of());
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("Gateway_WaitForHardware");
    }

    @Test
    public void testApproveOrder() {
        //given
        int PRICE = 1500;
        List<String> approvers = new LinkedList<>();
        approvers.add("Charlie");
        approvers.add("Snoopy");
        approvers.add("Woodstock");
        Map<String, Object> startVars = Map.of("price", PRICE, "approvers", approvers);

        //when
        ProcessInstanceEvent processInstance = startInstance("HardwareRequestProcess", startVars);

        //then
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("UserTask_ApproveOrder");

        //when
        completeUserTask(3, Map.of("approved", true));

        //then
        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("Gateway_1dpgaqe")
                .isWaitingAtElements("ServiceTask_CheckAvailability");
    }

    @Test
    public void testRejectOrder() {
        //given
        List<String> approvers = new LinkedList<>();
        approvers.add("Charlie");
        approvers.add("Snoppy");
        approvers.add("Woodstock");
        Map startVars = Map.of("approvers", approvers);

        //when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("HardwareRequestProcess", startVars, "UserTask_ApproveOrder");
        completeUserTask(2, Map.of("approved", true));
        completeUserTask(1, Map.of("approved", false));
        //then
        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("EndEvent_OrderRejected")
                .isCompleted();
    }

    @Test
    public void testHardwareStolen() {
        //when
        ProcessInstanceEvent processInstance =
                startInstanceBefore("HardwareRequestProcess", Map.of(), "ServiceTask_SendHardware");
        completeJobWithError("send-hardware", 1, "stolen");
        //then
        BpmnAssert.assertThat(processInstance)
                .isWaitingAtElements("SendTask_InformRequester");
        //when
        completeJob("inform-requester", 1, Map.of());
        //then
        BpmnAssert.assertThat(processInstance)
                .hasPassedElement("EndEvent_HardwareStolen")
                .isCompleted();
    }

    public void completeJobWithError(String type, int count, String errorCode) {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType(type)
                .maxJobsToActivate(count)
                .send().join();

        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if(activatedJobs.size() != count) {
            fail("No task found for: " + type);
        }

        for (ActivatedJob job:activatedJobs) {
            client.newThrowErrorCommand(job)
                    .errorCode(errorCode)
                    .send().join();
        }
    }

    public void completeUserTask(int count, Map<String,Object> variables) {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType("io.camunda.zeebe:userTask")
                .maxJobsToActivate(count)
                .send().join();
        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if (activatedJobs.size() != count) {
            fail("No user task found");
        }

        for (ActivatedJob job:activatedJobs) {
            client.newCompleteCommand(job)
                    .variables(variables)
                    .send().join();
        }
    }

    public ProcessInstanceEvent startInstanceBefore(String processId, Map<String, Object> variables, String startingpoint) {
        ProcessInstanceEvent processInstance = client.newCreateInstanceCommand()
                .bpmnProcessId(processId)
                .latestVersion()
                .variables(variables)
                .startBeforeElement(startingpoint)
                .send().join();
        BpmnAssert.assertThat(processInstance)
                .isStarted();
        return processInstance;
    }

    private ProcessInstanceEvent startInstance(String processId, Map<String, Object> startVars) {
        ProcessInstanceEvent processInstance = client. newCreateInstanceCommand()
                .bpmnProcessId("HardwareRequestProcess")
                .latestVersion()
                .variables(startVars)
                .send().join();

        BpmnAssert.assertThat(processInstance)
                .isStarted();

        return processInstance;
    }

    public void completeJob(String type, int count, Map<String, Object> variables) {
        ActivateJobsResponse activateJobsResponse = client.newActivateJobsCommand()
                .jobType(type)
                .maxJobsToActivate(count)
                .send().join();

        List<ActivatedJob> activatedJobs = activateJobsResponse.getJobs();
        if (activatedJobs.size() != count) {
            fail("No task found for: " + type);
        }

        for (ActivatedJob job:activatedJobs) {
            client.newCompleteCommand(job)
                    .variables(variables)
                    .send().join();
        }
    }

}
