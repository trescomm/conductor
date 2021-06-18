package com.netflix.conductor.contribs.tasks.aws;

import com.netflix.conductor.common.metadata.tasks.Task;
import com.netflix.conductor.common.metadata.tasks.TaskDef;
import com.netflix.conductor.common.metadata.tasks.TaskType;
import com.netflix.conductor.common.metadata.workflow.WorkflowTask;
import com.netflix.conductor.common.run.Workflow;
import com.netflix.conductor.core.exception.TerminateWorkflowException;
import com.netflix.conductor.core.execution.mapper.TaskMapper;
import com.netflix.conductor.core.execution.mapper.TaskMapperContext;
import com.netflix.conductor.core.utils.ParametersUtils;
import com.netflix.conductor.dao.MetadataDAO;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.*;

@Component
public class AWSLambdaTaskMapper implements TaskMapper {

    private static final Logger LOGGER = LoggerFactory.getLogger(AWSLambdaTaskMapper.class);

    private final ParametersUtils parametersUtils;

    private final MetadataDAO metadataDAO;

    public AWSLambdaTaskMapper(ParametersUtils parametersUtils, MetadataDAO metadataDAO) {
        this.parametersUtils = parametersUtils;
        this.metadataDAO = metadataDAO;
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.AWS_LAMBDA;
    }

    @Override
    public List<Task> getMappedTasks(TaskMapperContext taskMapperContext) throws TerminateWorkflowException {
        LOGGER.debug("TaskMapperContext {} in AWSLambdaTaskMapper", taskMapperContext);

        WorkflowTask taskToSchedule = taskMapperContext.getTaskToSchedule();
        taskToSchedule.getInputParameters().put("asyncComplete", taskToSchedule.isAsyncComplete());
        Workflow workflowInstance = taskMapperContext.getWorkflowInstance();
        String taskId = taskMapperContext.getTaskId();
        int retryCount = taskMapperContext.getRetryCount();

        TaskDef taskDefinition = Optional.ofNullable(taskMapperContext.getTaskDefinition())
                .orElseGet(() -> Optional.ofNullable(metadataDAO.getTaskDef(taskToSchedule.getName()))
                        .orElse(null));

        Map<String, Object> input = parametersUtils
                .getTaskInputV2(taskToSchedule.getInputParameters(), workflowInstance, taskId, taskDefinition);
        Boolean asynComplete = (Boolean) input.get("asyncComplete");

        Task lambdaTask = new Task();
        lambdaTask.setTaskType(taskToSchedule.getType());
        lambdaTask.setTaskDefName(taskToSchedule.getName());
        lambdaTask.setReferenceTaskName(taskToSchedule.getTaskReferenceName());
        lambdaTask.setWorkflowInstanceId(workflowInstance.getWorkflowId());
        lambdaTask.setWorkflowType(workflowInstance.getWorkflowName());
        lambdaTask.setCorrelationId(workflowInstance.getCorrelationId());
        lambdaTask.setScheduledTime(System.currentTimeMillis());
        lambdaTask.setTaskId(taskId);
        lambdaTask.setInputData(input);
        lambdaTask.getInputData().put("asyncComplete", asynComplete);
        lambdaTask.setStatus(Task.Status.SCHEDULED);
        lambdaTask.setRetryCount(retryCount);
        lambdaTask.setCallbackAfterSeconds(taskToSchedule.getStartDelay());
        lambdaTask.setWorkflowTask(taskToSchedule);
        lambdaTask.setWorkflowPriority(workflowInstance.getPriority());
        if (Objects.nonNull(taskDefinition)) {
            lambdaTask.setRateLimitPerFrequency(taskDefinition.getRateLimitPerFrequency());
            lambdaTask.setRateLimitFrequencyInSeconds(taskDefinition.getRateLimitFrequencyInSeconds());
            lambdaTask.setIsolationGroupId(taskDefinition.getIsolationGroupId());
            lambdaTask.setExecutionNameSpace(taskDefinition.getExecutionNameSpace());
        }
        return Collections.singletonList(lambdaTask);
    }
}
